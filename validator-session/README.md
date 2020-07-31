## Validator Session Component

### Consensus Algorithm Overview

To enable block validation, TON creates a validator list. This stage takes a fixed period of time configured in zerostate of blockchain. It is called validator session. This session is carried out in several rounds each aiming at generating a signed block.

The list of validators for each validation session is regulated by a specific smart contract that implements the elections logic. Such validation session is established for a number of rounds each of which ends with block committing or round skip. As a result of elections the list of nodes with their respective voting weights is created. A voting weight corresponds in direct ratio to a node stake value, so the larger the stake, the larger its weight. Then the weight is applied in decision making. Validator nodes perform voting by signing a block candidate by it's own private key. If a block gathers at least 2/3 of the weights total (cutoff weight), it automatically wins the particular voting.

Every round has several stages, in particular:

- block candidate generation
- approval
- vote
- signing

A round ends once a new signed block appears. To avoid controversy between validators over a new block, each round includes a configured number of several fixed-time attempts. The number of attempts is preconfigured in a blockchain's zerostate. If no consensus achieved within the maximum number of attempts, the round is considered failed, the committed block is skipped and won't be written to a blockchain. In general case when validators can't reach consensus for a few rounds, new validators elections can change the deadlock.

Each validator stores the validator session state and synchronizes it after each change with other nodes (i.e. creates a snapshot). Synchronization is implemented through the catchain protocol by message broadcasting (i.e. increments to the snapshot are made). The details of synchronization are described below in catchain protocol overview. Note, the syncrhonization is made not to all nodes after each change but to small subset of neighbour nodes. This subset is non constant and periodically updated, so this allows to minimize traffic between catchain nodes and deliver node's state transitively to all of them. The correctness of the state is controlled using signatures checking. The state consists of the current round state and of a list of previous round states (history of previous rounds).

The round state, in turn, consists of the list of attempts and of signatures of the elected commit candidate block (pre-committed block). Finally, each round attempt includes:

- sending of block candidates by validators to one another for approval;
- sending of a block used as a candidate for voting by primary for this attempt nodes to other nodes (based on node priority computed for this round attempt);
- sending of votes by validator nodes to one another.

It is noteworthy that to avoid deadlocks in voting and candidate generation, there is a node priority that depends on the number of nodes, round number, attempt number. So for each attempt priority changes and some validators gain privileges for the attempt. For example:

- priority to provide candidate blocks from a linked collator (only first `validatorSessionOptions::round_candidate` can propose a new candidate for approval);
- priority as ranging criteria for approval candidates;
- priority to choose an approved block for voting (only one node can do it).

Also, priority is used for delay calculation in the approval process during the round attempt (minimum priority corresponds to smaller delays for approval).

Another evident yet important point is that decision of each validator is checked by the rest of validators. The validator node public key is used to verify a signature. At the start of each validator session each validator receives information about all other validators including:

- the list of validators pre-created by the smart contract so that nodes could be addressed by indexes during the session;
- public keys of each validator;
- validator weights according to their stakes. Each time a validator receives block candidates, approvals or votes from other validator nodes it checks the signature (and in some cases priority as specified above) to validate that:
    - the sender is authorized to make the decision;
    - the decision was actually made by the sender.

### Consensus Stages

The stages (slots) of a round are following:

1. **Block candidate generation**. Each node having block generation priority for the round generates new block candidate which is requested from collator (see `validatorsession::ValidatorSession::Callback::on_generate_slot` for details). As soon the candidate appears, it is sent to other validator nodes (`validatorSession.candidate` > `validatorSession.message.submittedBlock`).
2. **Block candidates approval**. Blocks from the previous stage are collected on each validator node and are processed for approval. The approval process is not related to the consensus itself and is aimed at checking the blocks are not corrupted in terms of the bound data (see `validatorsession::ValidatorSession::Callback::on_candidate` of callback for details). As soon a block is approved, it is signed for approval by each validator. Then the `validatorSession.message.approvedBlock` message is broadcasted with the round number and the ID of the approving node. So, each validator is aware which node approved the sent block. The block is considered approved by a node when it recieves 2/3 of approval messages.
3. **Voting attempts**. Several block voting attempts are carried out. Each attempt is time limited and validators are supposed to fit into the slot. While votes for the current candidate are still **accepted** past the attempt deadline, it can cause a situation when two attempts result in different votes for multiple candidates, the case is considered further. The block candidate for voting is selected by the attempt’s main validator node and other nodes are notified by `validatorSession.message.voteFor` message (see `ValidatorSessionRoundState::generate_vote_for` for details; the main validator node which proposes the block for the attempt is provided by `ValidatorSessionDescriptionImpl::get_vote_for_author` method). The block selection involves two aspects: a) the node ranges approved blocks by nodes priority for the current attempt, so in each attempt the list is reordered, and b) the main validator node randomly selects a block for voting from this list. Each validator node processes the above-mentioned message and selects a block to vote for. A validator does not have to vote for the proposed block and is free to select another one. So the proposed block is more of a tip to provide an aim and to accelerate voting. Voting implies two things: a) signing block to mark it is voted by a particular validator node, and b) broadcasting this information in a `validatorSession.message.vote` message to the rest of validators. As soon as any approved block gets at least 2/3 of total validator weights in the course of voting, it becomes the candidate for signing (*pre-commit block*). Chances are that votes are received after the attempt end. In this case the late votes are processed simultaneously with the new attempt. Generally, votes for each finished attempt may be accepted past deadline; constituting no logical error, it allows finishing the round faster. And, if any of previous attempts ends up getting 2/3 of total weights or more even past its deadline, it still yields a new pre-commit candidate. In case of a conflict where two attempts propose different pre-commit blocks, the latest prevails. It is noteworthy that for performance reasons, a block vote obtained in one attempt is reused on the rest of attempts automatically.
4. **Block committing**. As soon as a round attempt yields a pre-committed block, the signing process starts. The validator that gets a pre-committed block in the current round signs it and broadcasts to other validators using a `validatorSession_message_commit` message (see `ValidatorSessionImpl::check_sign_slot` and related logic in `ValidatorSessionImpl::process_blocks` for details). All other validators receive the broadcast, check the sender signature and update their state (see `ValidatorSessionRoundState::action(…, validatorSession_message_commit` for details). At some moment, a particular validator gets not less than 2/3 signatures from other validators (in terms of validator weights). This leads to a) switching to a new round (increasing the round sequence number by 1), and b) committing the signed block to the blockchain (see `validatorsession::ValidatorSession::Callback::on_block_committed` for details).

If case of failure to get a signed block during the configured number of attempts (`ValidatorSessionOptions::max_round_attempts`), a new round starts and the failed one is marked as skipped (see `validatorsession::ValidatorSession::Callback::on_block_skipped`).

### Comparison with other solutions

Currently we believe that this scheme is quite similar to PBFT and to Tendermint schemes: a similar three-step phase pattern (Block approval, Voting, Precommitting) repeated until consensus is achieved. There are also some important details and differences. Right now we can mention the following:

- Fixed-time round attempts that require synchronized global clocks.
- The proposer node is selected based on the current global time (time is divided into 16-seconds slots, and the proposer is progressively changed each slot).
- Voting priorities are different (further research needed).
- Conflict resolution at voting is different, although this difference does not seem important.

The scheme is quite different from CBC Casper and similar approaches.

### Validator Session Protocol Messages & Structures

Validator session protocol consists of:

- incoming events: **validatorSession.round.Message** (which may be one of following sub-types: **validatorSession.message.submittedBlock, validatorSession.message.approvedBlock, validatorSession.message.rejectedBlock, validatorSession.message.voteFor, validatorSession.message.vote, validatorSession.message.precommit, validatorSession.message.commit, validatorSession.message.empty), validatorSession.blockUpdate, validatorSession.candidate**;
- required outgoing query **validatorSession.downloadCandidate** (with **validatorSession.candidate** as an event in subsequent event);
- internal structures which may be used in events and queries above.

Main flows:

1. Consensus stages
    1. Block candidate generation
        1. Validators which has a priority for blocks generation in the current round proposes block candidates using `validatorSession.message.submittedBlock` message.
        2. Each validator with block-candidate proposal priority can propose block in a limited time slot from the start of the round. Blocks proposed outside of the time slot will be ignored.
    2. Block candidates approval
        1. After receiving `validatorSession.message.submittedBlock` each validator starts checking the block for approval. In case of approval validator generates `validatorSession.message.approvedBlock`, in case of rejection - `validatorSession.message.rejectedBlock`.
    3. Voting attempts
        1. Validator which has a priority to propose block for voting ("vote-for" block) chooses randomly a block for voting from the list of approved blocks and generates `validatorSession.message.voteFor` message.
        2. Validator which receives `validatorSession.message.voteFor` checks if this message has been generated by a validator with "vote-for" block proposing priority. If so, uses the proposed "vote-for" block as a priority block for voting.
        3. In the first voting attempt validator votes on a block which has been received in a `[validatorSession.message.vote](http://validatorsession.message.vote)For` message. This is the fastest way of consensus decision making. Same voting on a block received in `validatorSession.message.voteFor` message will be done after reach of max voting attempts. As a result `validatorSession.message.vote` is generated with a vote on specific block.
        4. If the attempt is not first and is less than maximum number of voting attempts, then validator chooses block to vote on which has been proposed by validator with min priority for the round. As a result `validatorSession.message.vote` is generated with a vote on specific block.
        5. If validator has already voted on a block in one of previous round attempts, the validator generates `validatorSession.message.vote` with the same vote.
        6. When validator receives `validatorSession.message.vote`message, it checks if there are at least 2/3 of voting weights has been received for the voting block. If so, the validator generates `validatorSession.message.precommit` with a block which has been chosen as a candidate for committing. 
    4. Block committing
        1. When validator receives `validatorSession.message.precommit` message, it checks if there are at least 2/3 of voting weight have been received for the precommit block which has been proposed in a`validatorSession.message.precommit` message. If so,  a`validatorSession.message.commit` message is generated with a block for commit and a validator's signature.
        2. When validator receives `validatorSession.message.commit` message, it checks if there are at least 2/3 of voting weights have been received for to commit the proposed in the message block. If so, block is committed and new round is started.
        3. If during the maximum number of attempts any block has been committed, the round marked as skipped and new round is started.
2. Block candidate downloading
    - To speed up consensus processing there is a query `validatorSession.downloadCandidate` which may be sent by a validator to request via broadcast a block-candidate for a specific round with specific identifier. Should be generated by a validator if it doesn't have block candidate for further consensus processing.

Internal structures:

1. **validatorSession.config**
    - This structure is configured in zerostate and contains consensus configuration parameters which are the same for all validators.
        - `catchain_idle_timeout` : `double` - timeout between consensus iterations calls; each consensus iteration may result with new catchain block generation;
        - `catchain_max_deps` : `int` - maximum number of catchain blocks for merging during consensus iteration;
        - `round_candidates` : `int` - number of block candidates per round (during Block candidate generation stage);
        - `next_candidate_delay` : `double` - delay between proposing of block candidates;
        - `round_attempt_duration` : `int` - round attempt duration in milliseconds;
        - `max_round_attempts` : `int` - maximum number of attempts per round;
        - `max_block_size` : `int` - max size of the block;
        - `max_collated_data_size` : `int` - max size of collated data of the block.
2. **validatorSession.candidateId**
    - This internal structure is used as a part of `validatorSession.downloadCandidate` query and contains block-candidate hashes.
        - `src` : `int256` - hash of a public key of the validator which generated the block-candidate;
        - `root_hash` : `int256` - block's root hash;
        - `file_hash` : `int256` - block's file hash;
        - `collated_data_file_hash` : `int256` - block's collated data hash.

Events:

1. **validatorSession.round.Message**
    - **validatorSession.message.submittedBlock**
        - This messages is related to Block candidate generation) stage. Informs that validator has generated new block-candidate and submitted it to a catchain. Each validator has limited time slot for block-candidate generation (and proposing). This time slot is computed according to the node priority which depends on round and current time. During the block generation stage there may be up to `round_candidates` block-candidates. New candidate may be generated if during the `round_attempt_duration` current consensus round is not finished (new block is not committed). The message has following structure:
            - `round` : `int` - number of the round when new block-candidate appears;
            - `root_hash` : `int256` - block-candidate root hash;
            - `file_hash` : `int256` - block-candidate file hash;
            - `collated_data_file_hash` : `int256` - block-candidate data hash.
        - Actions: this message has to be generated when validator generates new block-candidate
    - **validatorSession.message.approvedBlock**
        - This message is related to Block candidate approval stage. The message informs that validator has approved the block `candidate` in the specific `round`. Each validator chooses list of blocks for approval. Only blocks which have not been previously approved blocks are included in the list. This list is sorted by node priority. The approval itself is done by a validator (not a validator session). The block is approved only from specific time (CandidateDecision::ok_from) computed by a validator. Block approval is initiated immediately after checking of block candidate and signing it. The message has following structure:
            - `round`: `int` - number of the round when `candidate` block has been approved;
            - `candidate` : `int256` - hash of block-candidate;
            - `signature` : `bytes` - validator's signature.
        - Actions: this message has to be generated when validator approves one or several block candidates.
    - **validatorSession.message.rejectedBlock**
        - This message is related to Block candidate approval stage. The message informs that validator has checked the block `candidate` and rejects it. This message is ignored by consensus itself (has no side effects except of logging if the message has been received by a validator). The message has following structure:
            - `round`: `int` - number of the round when `candidate` block has been rejected;
            - `candidate` : `int256` - hash of block-candidate;
            - `reason` : `bytes` - rejection reason (validator specific).
        - Actions: this message has to be generated when validator checks of the block have been failed. The message is not transitive, and should be sent only if current validator rejects the block-candidate.
    - **validatorSession.message.voteFor**
        - This message is related to Voting attempts stage. The message can be sent only by a validator which has priority to generate "vote-for" candidate for the attempt. The "vote-for" block is selected randomly from approved blocks by a validator with "vote-for" priority for the current attempt.
            - `round` : `int` - number of the round;
            - `attempt` : `int` - number of an attempt where `candidate` block will be chosen as "vote-for" block;
            - `candidate` : `int256` - block-candidate's hash.
        - Actions: this message has to be generated if:

            a) node has "vote-for" block generation priority for this attempt;

            b) node has at least one approved block;

            c) has no precommitted block;

            d) has not sent `validatorSession.message.voteFor` during this attempt..

    - **validatorSession.message.vote**
        - This message is related to Voting attempts stage. The message is sent to inform that a validator votes for a candidate. The message has following structure:
            - `round` : `int` - number of the round;
            - `attempt` : `int` - number of an attempt where validator voted for `candidate` block;
            - `candidate` : `int256` - block-candidate's hash.
        - Validator votes for block in one of following case (logical OR):

            a) block has been already voted in one of previous attempts during the round;

            b) block has been proposed by a validator with min priority for the round if current attempt is not first one and less than max round attempts;

            c) block has been proposed in `validatorSession.message.voteFor`message by a validator with "vote-for" generation priority if attempt is the first or is greater than max round attempts (opposite to the case (b) above).

        - Actions: this message has to be generated if there is no precommitted block during the current attempt and one of validator voting cases triggered.
    - **validatorSession.message.precommit**
        - This message is related to Voting attempts stage. The message is sent by each validator to notify that the validator has selected voted block as a precommitted. Precommitted block has to be voted by at lease 2/3 of total validators weight. Note, for some reason telegram's consensus ignores the case when node precommit block is not equal to candidate for this round and only prints warning to log about such case. The message has following structure:
            - `round` : `int` - number of the round;
            - `attempt` : `int` - number of an attempt where validator has chosen `candidate` block as a block for precommit;
            - `candidate` : `int256` - block-candidate's hash.
        - Actions: this message has to be generated when all conditions below are met:

            a) node has not sent `validatorSession.message.precommit` in this attempt;

            b) there is a block with 2/3 of total validators weight.

    - **validatorSession.message.commit**
        - This message is related to Block committing stage. This message informs that validator signed the block `candidate` as a block to be committed for a round. The message has following structure:
            - `round`: `int` - number of the round
            - `candidate` : `int256` - block-candidate's hash;
            - `signature` : `bytes` - signature of the validator.
    - **validatorSession.message.empty**
        - This is special message which is used as a marker to stop generation of messages during synchronization from internal validator state to the list of incremental messages which will be sent to other validators. The message has following structure:
            - `round` : `int` - number of the round where synchronizations takes place;
            - `attempt` : `int` - number of an attempt where synchronization takes place.
        - Actions: this message has to be generated to indicate there is no additional messages to sync state (in attempt and round).
2. **validatorSession.blockUpdate**
    - This message is the root message for validator session blocks update. It is packed to a payload of `[catchain.block.data.vector](https://www.notion.so/tonlabs/TON-Consensus-draft-8f93a062d73c433bba6f6eec60cd313e#58372e4545a345918284fee33f7d7e2d)`. This message is generated after each consensus iteration and has following structure: ****
        - `ts` : `long` - timestamp for block's update generation (unix time, global);
        - `actions` : `vector validatorSession.round.Message` - list of `[validatorSession.round.Message](https://www.notion.so/tonlabs/TON-Consensus-draft-8f93a062d73c433bba6f6eec60cd313e#708f5153766240e795345c5280fee4a9)` with incremental updates for consensus state synchronization on other validators;
        - `state` : `int` - state's hash after actions applying; is used like a checksum for sanity checks (computed recursively for session state; not a trivial linear hash of the buffer with actions).
3. **validatorSession.candidate**
    - This message is sent to a catchain as a broadcast when new candidate block appears (after validator `on_generate_slot` callback). Also, it may be sent at a start of catchain work on each validator for all approved by this validator blocks as a candidates to catchain (from validator `get_blocks_approved_by` and `get_approved_candidate`). This message initiates blocks approval if received in a broadcast and has following structure:
        - `src` : `int256` - validator which has generated a block-candidate;
        - `round` : `int` - number of a round where block-candidate appears;
        - `root_hash` : `int256` - root hash of block-candidate;
        - `data` : `bytes` - block-candidate's data;
        - `collated_data` : `bytes` - block-candidate's collated data.

Queries:

1. **validatorSession.downloadCandidate**
    - This messages is used to request via broadcast a block-candidate for specific round with specific identifier. Should be generated by a validator if it doesn't have block candidate for further consensus processing.
    - Request:
        - `round` : `int` - number of the round where block-candidate appears;
        - `id` : `validatorSession.candidateId` - block-candidate identifier.
    - Side effect:
        - `validatorSession.candidate` - block-candidate will be sent as an event during the request processing.
