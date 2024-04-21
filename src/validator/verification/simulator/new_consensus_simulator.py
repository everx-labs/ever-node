import random
import copy
from pprint import pprint
from datetime import datetime, timedelta

###################################################################################################
# Constants
###################################################################################################
SIMULATION_TIME = timedelta(minutes=10) # time of the simulation
SIMULATION_STEP = timedelta(milliseconds=100) # simulation step period
DUMP_STEP = timedelta(milliseconds=500) # simulation dump period

SHARD_VALIDATORS_COUNT = 7 # number of validators in shard
MC_VALIDATORS_COUNT = 100 # number of validators in masterchain
SHARDS_COUNT = 8 # number of shards in workchain
WORKCHAINS_COUNT = 3 # number of workchains

CANDIDATE_DELAY = 3000 # time for candidate collation
MAX_CANDIDATES_COUNT = 3 # maximum number of candidates per BFT round
EMPTY_BLOCK_DELAY = CANDIDATE_DELAY * MAX_CANDIDATES_COUNT # delay for skip block event
MIN_CANDIDATE_GENERATION_DELAY = 0.3
MAX_CANDIDATE_GENERATION_DELAY = 1.0
VALID_BLOCK_THRESHOLD = 0.95 # probability to select block as valid in BFT
MALLICIOUS_BLOCK_THRESHOLD = 0.5 # probability to generate mallision block which will be ignored by shard
MIN_CANDIDATE_SHARD_DELIVERY_DELAY = 0.2 # min delay for delivery candidate within shard
MAX_CANDIDATE_SHARD_DELIVERY_DELAY = 0.8 # max delay for delivery candidate within shard
MIN_CANDIDATE_WORKCHAIN_DELIVERY_DELAY = MIN_CANDIDATE_SHARD_DELIVERY_DELAY # min delay for delivery candidate within workchain
MAX_CANDIDATE_WORKCHAIN_DELIVERY_DELAY = MAX_CANDIDATE_SHARD_DELIVERY_DELAY # max delay for delivery candidate within workchain
MIN_CANDIDATE_WORKCHAIN_DELIVERY_RELIABILITY = 0.9
MAX_CANDIDATE_WORKCHAIN_DELIVERY_RELIABILITY = 1.0
BFT_CUTOFF_WEIGHT = 0.67

CANDIDATE_DELIVERY_ACK_NEIGHBOURS_ROTATION_PERIOD = 3000 # neighbours of broadcast protection flow rotation period
CANDIDATE_DELIVERY_ACK_NEIGHBOURS_COUNT = int(SHARDS_COUNT * SHARD_VALIDATORS_COUNT / 5) # number of neighbours for broadcast protection delivery

MIN_CANDIDATE_DELIVERY_ACK_DELAY = MIN_CANDIDATE_SHARD_DELIVERY_DELAY
MAX_CANDIDATE_DELIVERY_ACK_DELAY = MAX_CANDIDATE_SHARD_DELIVERY_DELAY
MIN_CANDIDATE_DELIVERY_ACK_RELIABILITY = 0.9
MAX_CANDIDATE_DELIVERY_ACK_RELIABILITY = 1.0

DELIVERY_CUTOFF_WEIGHT = 0.5001 # weight for validators received broadcast

MIN_SHARD_BLOCK_MC_DELIVERY_DELAY = 0.1 # min time for delivery committed block from shard to MC
MAX_SHARD_BLOCK_MC_DELIVERY_DELAY = 0.4
MIN_CANDIDATE_STATUS_MC_DELIVERY_DELAY = MIN_SHARD_BLOCK_MC_DELIVERY_DELAY
MAX_CANDIDATE_STATUS_MC_DELIVERY_DELAY = MAX_SHARD_BLOCK_MC_DELIVERY_DELAY
WORKCHAIN_TO_MASTERCHAIN_ACK_MODULO = 3

SELF_RECHECK_DELAY = 0.1 #delay for rechecking a message on the same node (like broadcast delivery results)
SELF_MAX_RECHECK_ATTEMPTS = 5 #max number of attempts for self rechecking

VERIFICATOR_SELECTION_THRESHOLD = 0.01 #probability to be a verificator
MIN_VERIFICATION_DELIVERY_DELAY = MIN_CANDIDATE_SHARD_DELIVERY_DELAY
MAX_VERIFICATION_DELIVERY_DELAY = MAX_CANDIDATE_SHARD_DELIVERY_DELAY
MIN_VERIFICATION_DELIVERY_RELIABILITY = MIN_CANDIDATE_DELIVERY_ACK_RELIABILITY
MAX_VERIFICATION_DELIVERY_RELIABILITY = MAX_CANDIDATE_DELIVERY_ACK_RELIABILITY
MALLICIOUS_VERIFIER_THRESHOLD = 0.05

###################################################################################################
# Global context
###################################################################################################
global_time = datetime.fromtimestamp(0)

###################################################################################################
# Utils
###################################################################################################

# Log
def log_print(s):
  print("{:10.3f} | {}".format(time_to_seconds(get_time() - datetime.fromtimestamp(0)), s))

# Convert time to seconds
def time_to_seconds(time):
  return time.microseconds / 1000000.0 + time.seconds

# Get scaled random value
def scaled_rand(min_value, max_value):
  if max_value == min_value:
    return min_value
  normalized_value = random.uniform(0, 1)
  return min_value + (max_value - min_value) * normalized_value

# Global time
def get_time():
  return global_time

# Create node messages queue
def create_message_queue():
  return []

# Put message to queue
def put_message_to_queue(queue, msg):
  queue.append(msg)

# Run messages in message queue
def process_messages(queue, node, time):
  msg_count = len(queue)

  for i in range(msg_count):
    msg = queue.pop(0)
    msg(node)

###################################################################################################
# MC state
###################################################################################################

def create_mc_state():
  state = {
    'workchains': {},
    'verifications': {},
  }
  return state

def get_mc_workchain_state(mc_state, workchain_id):
  if workchain_id in mc_state['workchains']:
    return mc_state['workchains'][workchain_id]

  wc_state = {
    'shards': {}
  }
  mc_state['workchains'][workchain_id] = wc_state

  return wc_state

def get_mc_shard_state(mc_state, workchain_id, shard_id):
  workchain = get_mc_workchain_state(mc_state, workchain_id)

  if shard_id in workchain['shards']:
    return workchain['shards'][shard_id]

  shard = {
    'top_block': None,
    'height': -1,
    'block_by_height': {},
    'blocks': {},
  }

  workchain['shards'][shard_id] = shard

  return shard

###################################################################################################
# Node
###################################################################################################

# Create shard node
def create_shard_node(shard_id, source_id, shard, workchain, simulator_state):
  node_id = simulator_state['node_id_counter']
  simulator_state['node_id_counter'] += 1
  node = {
    'shard_id' : shard_id,
    'source_id': source_id,
    'id' : node_id,
    'shard': shard,
    'workchain': workchain,
    'simulator_state': simulator_state,
    'block_number' : 0,
    'message_queue': create_message_queue(),
    'delayed_actions': [],
    'round': -1,
    'candidate_generated': False,
    'block_approvers': {},
    'block_delivered_sources': {},
    'block_delivery_mc_notified': {},
    'candidate_delivery_neighbours': None,
    'candidate_delivery_neighbours_next_rotation': get_time(),
  }

  if is_masterchain(node):
    node['mc_state'] = create_mc_state()

  node['workchain']['nodes'].append(node)
  node['workchain']['node_by_id'][node_id] = node

  return node

# Put message to node's message queue
def put_message_to_node_queue(node, msg, delay=0.0, reliability=1.0):
  reliability_rnd = random.uniform(0, 1)
  if reliability_rnd < 1.0 - reliability:
    return #ignore delivery

  if delay > 0.0:
    delayed_action = lambda node: put_message_to_node_queue(node, msg)
    delayed_action_time = get_time() + timedelta(milliseconds=int(delay * 1000))
    node['delayed_actions'].append({
      'time': delayed_action_time,
      'action': delayed_action,
    })
    return

  #todo: add node messages limit
  node['message_queue'].append(msg)

# Put message to shard nodes queues
def put_message_to_neighbours_queue(nodes, msg, min_delay=0.0, max_delay=0.0, min_reliability=1.0, max_reliability=1.0):
  for node in nodes:
    delay = scaled_rand(min_delay, max_delay)
    reliability = scaled_rand(min_reliability, max_reliability)
    put_message_to_node_queue(node, msg, delay=delay, reliability=reliability)

# Process delayed actions
def process_delayed_actions(node, time):
  actions = node['delayed_actions']
  node['delayed_actions'] = []

  for action_decl in actions:
    if action_decl['time'] <= time:
      node['delayed_actions'].append(action_decl)
      continue
    action_decl['action'](node)

# New round for node
def new_bft_round(node, time):
  node['start_round_time'] = time
  node['round'] += 1
  node['candidate_generated'] = False

  sources_count = len(node['shard']['nodes'])
  priority = node['round'] - node['source_id']
  if priority < 0:
    priority += sources_count
  priority = priority % sources_count

  node['end_round_time'] = time + timedelta(milliseconds=EMPTY_BLOCK_DELAY)
  node['collation_time'] = time + timedelta(milliseconds=CANDIDATE_DELAY * priority)

  #log_print("new round={} for {}:{}:{} node={} priority={}".format(node['round'], node['workchain']['id'], node['shard']['id'], node['block_number'],
  #  node['id'], priority))

# Commit block
def commit_block(node, block):
  round = block['round']

  if round != node['round']:
    return

  shard = node['shard']

  #log_print("commit block #{} for shard {}:{}:{} from node #{}; round={}".format(block['id'], shard['workchain']['id'], shard['id'], block['height'], block['created_by']['id'], round))

  if round in shard['round_candidate_selection']:
    assert shard['round_candidate_selection'][round] != 'empty_block'
    assert shard['round_candidate_selection'][round]['id'] == block['id']
  else:
    assert round == shard['round']

    shard['round_candidate_selection'][round] = block
    shard['top_block'] = block
    shard['block_height'] += 1
    shard['round'] += 1
    node['block_number'] += 1

  new_bft_round(node, get_time())

  if is_masterchain(node):
    apply_mc_block(node, block)
    #todo: broadcast block in MC
  else:
    # Send block to MC    
    put_message_to_workchain_queues(node['simulator_state']['workchains'][-1], lambda mc_node: new_shard_block_committed(mc_node, block),
      min_delay=MIN_SHARD_BLOCK_MC_DELIVERY_DELAY, max_delay=MAX_SHARD_BLOCK_MC_DELIVERY_DELAY)

# Commit empty block
def try_commit_empty_block(node, shard, round):
  if round in shard['round_candidate_selection']:
    return False

  if round != node['round']:
    return False

  log_print("commit empty block for node #{} in round {}".format(node['id'], round))

  shard['round_candidate_selection'][round] = 'empty_block'
  shard['round'] += 1

  new_bft_round(node, get_time())

  return True

def is_fork(node, block):
  round = block['round']
  shard = node['shard']

  if round in shard['round_candidate_selection']:
    if shard['round_candidate_selection'][round] == 'empty_block':
      return True

    if shard['round_candidate_selection'][round]['id'] != block['id']:
      return True

  return False

# Validate candidate
def validate_candidate(node, block):
  if not block['is_valid']:
    return False, False

  if is_fork(node, block):
    return False, False

  if is_masterchain(node):
    is_valid, need_to_wait = validate_mc_candidate(node, block)

    if not is_valid:
      return False, need_to_wait

  return True, False

# New candidate received within shard
def new_candidate_received_within_shard(node, block, attempt=0):
  #log_print('new candidate {} from node #{} has been received via shard by node #{}'.format(block['id'], block['created_by']['id'], node['id']))
  round = block['round']
  if round < node['round']:
    return

  is_valid, need_to_wait = validate_candidate(node, block)

  if not is_valid:
    if need_to_wait and attempt < SELF_MAX_RECHECK_ATTEMPTS:
      log_print("recheck block candidate {}:{}:{} with ID #{} in {:.3f}s".format(block['workchain_id'], block['shard_id'], block['height'], block['id'], SELF_RECHECK_DELAY))
      put_message_to_node_queue(node, lambda receiver_node: SELF_RECHECK_DELAY(receiver_node, block, attempt + 1), delay=SELF_RECHECK_DELAY)
    return

  put_message_to_shard_queues(node['shard'], lambda receiver_node: new_candidate_approved(receiver_node, node, block), min_delay=MIN_CANDIDATE_SHARD_DELIVERY_DELAY, max_delay=MAX_CANDIDATE_SHARD_DELIVERY_DELAY)

# New candidated approved by node
def new_candidate_approved(receiver_node, sender_node, block):
  round = block['round']

  if round < receiver_node['round']:
    return

  if round > receiver_node['round']:
    put_message_to_node_queue(receiver_node, lambda receiver_node: new_candidate_approved(receiver_node, sender_node, block), delay=SELF_RECHECK_DELAY)
    return

  approvers = {}
  block_id = block['id']
  sender_id = sender_node['id']

  if block_id in receiver_node['block_approvers']:
    approvers = receiver_node['block_approvers'][block_id]
  else:
    receiver_node['block_approvers'][block_id] = approvers

  if sender_id in approvers:
    return

  approvers[sender_id] = True

  approves_count = len(approvers)
  total_approvers_count = len(receiver_node['shard']['nodes'])
  weight = approves_count / float(total_approvers_count)

  if weight < BFT_CUTOFF_WEIGHT:
    #log_print("node {}:{}:{} with ID #{} is waiting for consensus in round {} for block #{} created by node #{}; current weight is {:.2f}%".format(block['workchain_id'], block['shard_id'], block['height'], receiver_node['id'],
    #  round, block['id'], block['created_by']['id'], weight * 100.0))
    return

  if is_fork(receiver_node, block):
    return

  commit_block(receiver_node, block)

# New candidate received within workchain
def new_candidate_received_within_workchain(node, block):
  #log_print('new candidate {} from node #{} has been received via workchain by node #{}'.format(block['id'], block['created_by']['id'], node['id']))
  put_message_to_neighbours_queue(node['candidate_delivery_neighbours'], lambda receiver_node: new_candidate_delivery_accepted(receiver_node, [node], block),
    min_delay=MIN_CANDIDATE_DELIVERY_ACK_DELAY, max_delay=MAX_CANDIDATE_DELIVERY_ACK_DELAY,
    min_reliability=MIN_CANDIDATE_DELIVERY_ACK_RELIABILITY, max_reliability=MAX_CANDIDATE_DELIVERY_ACK_RELIABILITY)

  # select verificator
  if random.uniform(0, 1) < VERIFICATOR_SELECTION_THRESHOLD:
    verify_block(node, block)

# Delivery acceptance by node within workchain
def new_candidate_delivery_accepted(receiver_node, sender_nodes, block):
  #log_print('new candidate {} ACK has been received via workchain by node #{} from {} senders'.format(block['id'], receiver_node['id'], len(sender_nodes)))

  delivery_sources = {}
  block_id = block['id']
  if block_id in receiver_node['block_delivered_sources']:
    delivery_sources = receiver_node['block_delivered_sources'][block_id]
  else:
    receiver_node['block_delivered_sources'][block_id] = delivery_sources

  # Compute delivery sources before applying update
  start_delivery_sources = 0
  for node_id, status in delivery_sources.items():
    if not status:
      continue
    start_delivery_sources += 1

  # Apply update  
  for sender_node in sender_nodes:
    if sender_node == receiver_node:
      return
    sender_id = sender_node['id']
    if sender_id in delivery_sources:
      continue
    delivery_sources[sender_id] = True

  delivery_sources[receiver_node['id']] = True

  # Compute delivery sources after applying update
  sender_nodes = []
  end_delivery_sources = 0

  for node_id, status in delivery_sources.items():
    if not status:
      continue
    sender_nodes.append(receiver_node['workchain']['node_by_id'][node_id])
    end_delivery_sources += 1

  # Continue propagation in case of new source appeared
  if end_delivery_sources > start_delivery_sources:
    put_message_to_neighbours_queue(receiver_node['candidate_delivery_neighbours'], lambda receiver_node: new_candidate_delivery_accepted(receiver_node, sender_nodes, block),
      min_delay=MIN_CANDIDATE_DELIVERY_ACK_DELAY, max_delay=MAX_CANDIDATE_DELIVERY_ACK_DELAY,
      min_reliability=MIN_CANDIDATE_DELIVERY_ACK_RELIABILITY, max_reliability=MAX_CANDIDATE_DELIVERY_ACK_RELIABILITY)

  block_delivery_nodes_count = len(delivery_sources)
  block_delivery_total_nodes_count = len(receiver_node['workchain']['nodes'])
  weight = block_delivery_nodes_count / float(block_delivery_total_nodes_count)

  if weight >= DELIVERY_CUTOFF_WEIGHT:
    #log_print("block #{} has been received by {:.1f}% of workchain ({}/{})".format(block['id'], weight * 100.0, block_delivery_nodes_count, block_delivery_total_nodes_count))

    if not block_id in receiver_node['block_delivery_mc_notified'] and receiver_node['source_id'] % WORKCHAIN_TO_MASTERCHAIN_ACK_MODULO == 0:
      #log_print("block #{} has been received by {:.1f}% of workchain; send ACK to MC".format(block['id'], weight * 100.0))

      receiver_node['block_delivery_mc_notified'][block_id] = True
      put_message_to_workchain_queues(receiver_node['simulator_state']['workchains'][-1], lambda node: candidate_delivered_to_workchain(node, block, weight),
        min_delay=MIN_CANDIDATE_STATUS_MC_DELIVERY_DELAY, max_delay=MAX_CANDIDATE_STATUS_MC_DELIVERY_DELAY)

# Generate candidate
def generate_candidate(node, round, time):
  # generate new block
  block = {
    'id': node['simulator_state']['block_id_counter'],
    'workchain_id': node['workchain']['id'],
    'shard_id': node['shard']['id'],
    'created_by': node,
    'height': node['shard']['block_height'] + 1,
    'prev': node['shard']['top_block'],
    'round': round,
    'is_valid': random.uniform(0, 1) < VALID_BLOCK_THRESHOLD,
    'is_mallicious': random.uniform(0, 1) < MALLICIOUS_BLOCK_THRESHOLD,
    'creation_time': get_time(),
  }

  log_print('generate candidate {}:{}:{} by collator node #{}; round={}'.format(block['workchain_id'], block['shard_id'], block['height'], node['id'], round))

  node['simulator_state']['block_id_counter'] += 1

  if is_masterchain(node):
    generate_mc_candidate(node, block)

  put_message_to_shard_queues(node['shard'], lambda node: new_candidate_received_within_shard(node, block), min_delay=MIN_CANDIDATE_SHARD_DELIVERY_DELAY, max_delay=MAX_CANDIDATE_SHARD_DELIVERY_DELAY)

  if not is_masterchain(node):
    put_message_to_workchain_queues(node['workchain'], lambda node: new_candidate_received_within_workchain(node, block),
      min_delay=MIN_CANDIDATE_WORKCHAIN_DELIVERY_DELAY, max_delay=MAX_CANDIDATE_WORKCHAIN_DELIVERY_DELAY,
      min_reliability=MIN_CANDIDATE_WORKCHAIN_DELIVERY_RELIABILITY, max_reliability=MAX_CANDIDATE_WORKCHAIN_DELIVERY_RELIABILITY)

# Check if not is in masterchain
def is_masterchain(node):
  return node['workchain']['id'] == -1

# Get new block within MC state for shard
def get_new_shard_block(node, block):
  assert is_masterchain(node)

  block_id = block['id']
  block_height = block['height']
  workchain_id = block['workchain_id']
  shard_id = block['shard_id']

  mc_state = node['mc_state']
  shard_state = get_mc_shard_state(mc_state, workchain_id, shard_id)

  if block_id in shard_state['blocks']:
    return shard_state['blocks'][block_id], False;

  block_decl = {
    'id': block_id,
    'block': block,
    'delivery_weight': 0.0,
  }

  shard_state['blocks'][block_id] = block_decl

  return block_decl, True;

# New shard block appeared
def new_shard_block_committed(node, block):
  block_decl, is_created = get_new_shard_block(node, block)

  #log_print("new shard block {}:{}:{} with ID #{} appeared in MC (mcnode is #{})".format(block['workchain_id'], block['shard_id'], block['height'], block['id'], node['id']))

  if not is_created:
    assert block['id'] == block_decl['id']

  block_id = block['id']
  block_height = block['height']
  workchain_id = block['workchain_id']
  shard_id = block['shard_id']

  mc_state = node['mc_state']
  shard_state = get_mc_shard_state(mc_state, workchain_id, shard_id)

  if block_height in shard_state['block_by_height']:
    decl = shard_state['block_by_height'][block_height]

    if block_id != decl['id']:
      log_print("conflict for shard {}:{}:{} with ID {} and candidate {}:{}:{} with ID {}".format(decl['block']['workchain_id'], decl['block']['shard_id'],
        decl['block']['height'], decl['id'], block['workchain_id'], block['shard_id'], block['height'], block['id']))
      assert block_id == decl['id']

    return

  shard_state['block_by_height'][block_height] = block_decl

  #log_print("new shard block {}:{}:{} with ID #{} appeared in MC (mcnode is #{}); {} heights are registered".format(block['workchain_id'], block['shard_id'],
  #  block['height'], block['id'], node['id'], len(shard_state['block_by_height'])))

def candidate_delivered_to_workchain(node, block, weight):
  block_decl, is_created = get_new_shard_block(node, block)

  if weight > block_decl['delivery_weight']:
    block_decl['delivery_weight'] = weight

def get_blocks_heights(blocks, min_height):
  heights = []

  for block_height, block_decl in blocks.items():
    if block_height <= min_height:
      continue
    heights.append(block_height)

  heights.sort(reverse=True)

  return heights

def dump_mc_block_state(state):
  for workchain_id, workchain in state['workchains'].items():
    log_print("  workchain {}:".format(workchain_id))
    for shard_id, shard in workchain['shards'].items():
      top_block = shard['top_block']

      if top_block:
        top_block_string = "#{}".format(top_block['id'])
        blamed_string = ""

        if 'is_blamed' in shard and shard['is_blamed']:
          blamed_string = "; BLAMED!"
        log_print("    shard {}: top block {:5} with height {}{}".format(shard_id, top_block_string, top_block['height'], blamed_string))
      else:
        log_print("    shard {}: N/A".format(shard_id))

def dump_mc_node_state(state):
  dump_mc_block_state(state)

def apply_mc_block(node, mc_block):
  #log_print("applying MC block {}:{}:{} with ID #{} for node #{}".format(mc_block['workchain_id'], mc_block['shard_id'], mc_block['height'], mc_block['id'], node['id']))

  mc_node_state = node['mc_state']
  mc_block_state = mc_block['mc_state']
  min_creation_time = get_time()

  for workchain_id, block_workchain in mc_block_state['workchains'].items():
    node_workchain = mc_node_state['workchains'][workchain_id]

    for shard_id, block_shard in block_workchain['shards'].items():
      node_shard = node_workchain['shards'][shard_id]

      node_shard['top_block'] = block_shard['top_block']

      if block_shard['top_block']:
        if node_shard['top_block']:
          block_it = block_shard['top_block']
          block_it_end = node_shard['top_block']
          prev_height = node_shard['top_block']['height']

          while block_it != block_it_end:
            if min_creation_time > block_it['creation_time']:
              min_creation_time = block_it['creation_time']

            block_it = block_it['prev']

        if min_creation_time > block_shard['top_block']['creation_time']:
          min_creation_time = block_shard['top_block']['creation_time']

        node_shard['height'] = block_shard['top_block']['height']
      else:
        node_shard['height'] = -1

  #dump with minimized spam
  if not mc_block['id'] in node['shard']['shard_mc_dumps']:
    log_print("dump MC state {}:{}:{} after applying of block #{} on node #{} (max_latency={:.3f}s):".format(mc_block['workchain_id'], mc_block['shard_id'], mc_block['height'], mc_block['id'], node['id'],
      time_to_seconds(get_time() - min_creation_time)))

    node['shard']['shard_mc_dumps'][mc_block['id']] = True
    dump_mc_node_state(mc_node_state)

def check_verifications(mc_node, mc_block, cur_block, new_block_it, check_only=False):
  mc_state = mc_node['mc_state']
  verifications = mc_state['verifications']

  while new_block_it:
    new_block = new_block_it
    new_block_it = new_block_it['prev']
    block_verifications = None
    block_id = new_block['id']

    if cur_block and cur_block['id'] == new_block['id']:
      break

    if not block_id in verifications:
      continue #no rejects from verifiers; todo: wait for at least 1 approve?

    block_verifications = verifications[block_id]

    if block_verifications['is_suspicious']:
      if not check_only:
        mallicious_block_detected(mc_node, mc_block, new_block, block_verifications['verifiers'])
      return False
    else:
      if new_block['is_mallicious'] and not check_only:
        log_print("!!! mallicious shard block {}:{}:{} with ID #{} to MC block {}:{}:{} has NOT been detected on node #{} (collator is node #{})".format(
          new_block['workchain_id'], new_block['shard_id'], new_block['height'], new_block['id'],
          mc_block['workchain_id'], mc_block['shard_id'], mc_block['height'], mc_node['id'], new_block['created_by']['id']))

  return True

def mallicious_block_detected(mc_node, mc_block, shard_block, verifications):
  log_print("!!! attempt to commit mallicious shard block {}:{}:{} with ID #{} to MC block {}:{}:{} has been detected on node #{} (collator is node #{})".format(
    shard_block['workchain_id'], shard_block['shard_id'], shard_block['height'], shard_block['id'],
    mc_block['workchain_id'], mc_block['shard_id'], mc_block['height'], mc_node['id'], shard_block['created_by']['id']))

  for verifier_id, verification in verifications.items():
    status_string = None

    if verification['status']:
      status_string = "approved"
    else:
      status_string = "rejected"

    log_print("  {} by verifier node #{}".format(status_string, verifier_id))

  mc_node['mc_state']['workchains'][shard_block['workchain_id']]['shards'][shard_block['shard_id']]['is_blamed'] = True

  #todo: reset shard!!!
  #todo: block verification for wide consensus
  #todo: slashing events

def validate_mc_candidate(node, mc_block):
  #log_print("validating MC block {}:{}:{} with ID #{} on node #{}".format(mc_block['workchain_id'], mc_block['shard_id'], mc_block['height'], mc_block['id'], node['id']))

  mc_node_state = node['mc_state']
  mc_block_state = mc_block['mc_state']

  for workchain_id, block_workchain in mc_block_state['workchains'].items():
    node_workchain = mc_node_state['workchains'][workchain_id]

    for shard_id, block_shard in block_workchain['shards'].items():
      node_shard = node_workchain['shards'][shard_id]
      cur_block = node_shard['top_block']
      new_block = block_shard['top_block']

      # check verifications
      if not check_verifications(node, mc_block, cur_block, new_block):
        return False, False

      if not cur_block:
        continue

      if not new_block:
        return False, False

      block_id = new_block['id']
      new_block_decl = node_shard['blocks'][block_id]

      if not new_block_decl:
        return False, True # block has not been delivered to this node yet

      if new_block_decl['delivery_weight'] < DELIVERY_CUTOFF_WEIGHT:
        return False, True #reject shard blocks which have no cutoff broadcast receive ACKs

      if new_block['height'] < cur_block['height']:
        return False, False

  return True, False

def generate_mc_candidate(node, mc_block):
  mc_state = node['mc_state']
  workchains_block_state = {}
  mc_block_state = {'workchains': workchains_block_state}

  mc_block['mc_state'] = mc_block_state

  for workchain_id, workchain in mc_state['workchains'].items():
    shards_block_state = {}
    workchain_block_state = {'shards' : shards_block_state}
    workchains_block_state[workchain_id] = workchain_block_state

    for shard_id, shard in workchain['shards'].items():
      shard_block_state = {'top_block': None}
      shards_block_state[shard_id] = shard_block_state
      unprocessed_heights = get_blocks_heights(shard['block_by_height'], shard['height'])
      cur_block = mc_state['workchains'][workchain_id]['shards'][shard_id]['top_block']

      while len(unprocessed_heights) > 0:
        height = unprocessed_heights.pop(0)
        block_decl = shard['block_by_height'][height]

        if block_decl['delivery_weight'] < DELIVERY_CUTOFF_WEIGHT:
          continue #ignore shard blocks which have no cutoff broadcast receive ACKs

        if not check_verifications(node, mc_block, cur_block, block_decl['block']):
          continue

        shard_block_state['top_block'] = block_decl['block']

        break

  log_print("dump MC state for block #{} during generation on node #{}:".format(mc_block['id'], node['id']))
  dump_mc_block_state(mc_block_state)

# MC node notification about verification status
# todo: garbage collection for verification statuses
def mc_notify_verification_status(mc_node, verifier_node, block, status):
  mc_state = mc_node['mc_state']
  verifications = mc_state['verifications']
  block_verifications = None
  block_id = block['id']

  if block_id in verifications:
    block_verifications = verifications[block_id]
  else:
    block_verifications = {
      'is_suspicious': False,
      'verifiers': {},
    }
    verifications[block_id] = block_verifications

  verifiers = block_verifications['verifiers']
  verifier_id = verifier_node['id']

  if verifier_id in verifiers:
    return #ignore duplicate verifications

  verification = {
    'status': status,
  }

  verifiers[verifier_id] = verification

  if not status:
    block_verifications['is_suspicious'] = True

# Verification
def verify_block(node, block):
  log_print("verifying block {}:{}:{} with ID #{} on node #{} (collated on node #{})".format(block['workchain_id'], block['shard_id'], block['height'], block['id'], node['id'], block['created_by']['id']))

  status = validate_candidate(node, block) and not block['is_mallicious']

  if random.uniform(0, 1) < MALLICIOUS_VERIFIER_THRESHOLD:
    status = True

  put_message_to_workchain_queues(node['simulator_state']['workchains'][-1], lambda mc_node: mc_notify_verification_status(mc_node, node, block, status),
    min_delay=MIN_VERIFICATION_DELIVERY_DELAY, max_delay=MAX_VERIFICATION_DELIVERY_DELAY, min_reliability=MIN_VERIFICATION_DELIVERY_RELIABILITY, max_reliability=MAX_VERIFICATION_DELIVERY_RELIABILITY)

# Do simulation step for node
def update_node(node, time):
  # Update candidate delivery neighbours

  if not node['candidate_delivery_neighbours'] or node['candidate_delivery_neighbours_next_rotation'] <= time:
    # rotate neighbours
    neighbours = []
    workchain = node['workchain']
    nodes_count = len(workchain['nodes'])
    for i in range(CANDIDATE_DELIVERY_ACK_NEIGHBOURS_COUNT):
      neighbour_id = random.randint(0, nodes_count-1)
      neighbours.append(workchain['nodes'][neighbour_id])
    node['candidate_delivery_neighbours_next_rotation'] = get_time() + timedelta(milliseconds=CANDIDATE_DELIVERY_ACK_NEIGHBOURS_ROTATION_PERIOD)
    node['candidate_delivery_neighbours'] = neighbours

  # Process queues
  process_delayed_actions(node, time)
  process_messages(node['message_queue'], node, time)

  # Do simplified BFT consensus iteration
  round = node['round']

  if not 'collation_time' in node:
    #log_print("start first round for node #{}".format(node['id']))
    new_bft_round(node, time)

  round = node['round']

  #while round in node['shard']['round_candidate_selection']:
  #  new_bft_round(node, time)
  #  round = node['round']
  
  if time >= node['end_round_time']:
    try_commit_empty_block(node, node['shard'], round)
  elif time >= node['collation_time'] and not node['candidate_generated']:
    node['candidate_generated'] = True
    put_message_to_node_queue(node, lambda node: generate_candidate(node, round, get_time()), delay=scaled_rand(MIN_CANDIDATE_GENERATION_DELAY, MAX_CANDIDATE_GENERATION_DELAY))

###################################################################################################
# Shard
###################################################################################################

# Create shard
def create_shard(shard_id, workchain, validators_count, simulator_state):
  nodes = {}
  shard = {
    'id': shard_id,
    'nodes': nodes,
    'workchain': workchain,
    'simulator_state': simulator_state,
    'block_height': -1,
    'round': 0,
    'round_candidate_selection': {}, #fake map to simplify BFT consensus phases
    'top_block': None,
    'shard_mc_dumps': {},
  }

  for i in range(validators_count):
    source_id = i
    nodes[i] = create_shard_node(shard_id, source_id, shard, workchain, simulator_state)

  return shard

# Put message to shard nodes queues
def put_message_to_shard_queues(shard, msg, min_delay=0.0, max_delay=0.0, min_reliability=1.0, max_reliability=1.0):
  for _, node in shard['nodes'].items():
    delay = scaled_rand(min_delay, max_delay)
    reliability = scaled_rand(min_reliability, max_reliability)
    put_message_to_node_queue(node, msg, delay=delay, reliability=reliability)

# Do simulation step for shard
def update_shard(shard, time):
  # Process nodes
  for _, node in shard['nodes'].items():
    update_node(node, time)

###################################################################################################
# Workchain
###################################################################################################

# Create workchain
def create_workchain(workchain_id, shards_count, validators_count, simulator_state):
  shards = {}
  workchain = {
    'id' : workchain_id,
    'shards' : shards,
    'nodes': [],
    'node_by_id': {},
    'simulator_state': simulator_state,
  }

  # Initialize shards
  
  for i in range(shards_count):
    shard_id = i
    shards[i] = create_shard(shard_id, workchain, validators_count, simulator_state)

  return workchain

# Put message to workchain's nodes' queues
def put_message_to_workchain_queues(workchain, msg, min_delay=0.0, max_delay=0.0, min_reliability=1.0, max_reliability=1.0):
  for _, shard in workchain['shards'].items():
    put_message_to_shard_queues(shard, msg, min_delay=min_delay, max_delay=max_delay, min_reliability=min_reliability, max_reliability=max_reliability)

# Do simulation step for workchain
def update_workchain(workchain, time):
  for _, shard in workchain['shards'].items():
    update_shard(shard, time)

###################################################################################################
# Simulator
###################################################################################################

# Simulator initialization
def create_simulator():
  workchains = {}
  simulator_state = {
    'workchains' : workchains,
    'node_id_counter': 0,
    'block_id_counter': 0,
  }

  # Initialize workchains

  for i in range(WORKCHAINS_COUNT):
    workchain_id = i
    workchains[i] = create_workchain(workchain_id, SHARDS_COUNT, SHARD_VALIDATORS_COUNT, simulator_state)

  workchains[-1] = create_workchain(-1, 1, MC_VALIDATORS_COUNT, simulator_state)

  # Create simulation state

  return simulator_state

# Simulation step
def update_simulator(state, time):
  # Update workchains
  for _, workchain in state['workchains'].items():
    update_workchain(workchain, time)

###################################################################################################
# Main
###################################################################################################

def main():
  global global_time

  # initialize simulator
  state = create_simulator()

  # do simulation loops
  start_time = datetime.fromtimestamp(0)
  end_time = start_time + SIMULATION_TIME
  time = start_time
  next_dump_time = time
  global_time = time

  while time <= end_time:
    # Update simulator
    update_simulator(state, time)

    # Dump simulator step
    if time >= next_dump_time or time + SIMULATION_STEP > end_time:
      time_delta = time_to_seconds(time - start_time)
      end_time_delta = time_to_seconds(end_time - start_time)
      log_print("{:.2f}% time is {:.2f}s".format(100.0 * time_delta / end_time_delta, time_delta))
      #plog_print(state)
      next_dump_time += DUMP_STEP
      #put_message_to_workchain_queues(state['workchains'][0], lambda node: log_print("node={} time={:.3f}s".format(node['id'], time_to_seconds(get_time()-start_time))),
      #  min_delay=0.2, max_delay=1.0, min_reliability=0.1, max_reliability=0.2)

    # Increment simulation time
    time += SIMULATION_STEP
    global_time = time

main()
