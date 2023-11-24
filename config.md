`remp` section
------------

* `service_enabled`: possible values `true` and `false`. 
Enables participation in validator REMP protocols. Default value is `true`.

  The service allows the node to validate in REMP networks, but does not affect validation
  in non-REMP networks. So if the Network REMP capability is turned off now but may be activated 
  in the future, leave the default value.

  However, REMP protocols take some resources from the node even if the REMP capability is
  turned off. If the node is not expected to be a validator in REMP network,
  set this to `false`.

* `client_enabled`: possible values `true` and `false`. Default value `true`.

  Enables participation in client REMP protocols. With this option
  set to `false`, the node may not send external messages to 
  REMP network. As with `service_enabled` parameter, the client service is transparent
  for non-REMP networks, but may take extra hardware resources.

* `message_queue_max_len`: non-negative integer value.
  When specified, sets maximal number of external messages
  which can be handled by REMP simultaneously. Handling means all 
  message processing stages from its receiving by node till
  its expiration for replay protection purposes. The message count
  is performed for each shard separately.

  May be used to avoid node overloading by external messages. If the  
  queue becomes too long, all new messages are rejected, until some of the 
  messages from the queue become outdated (that is, their replay protection 
  period expires).

  If the value is not specified, no check of the message queue length is performed.
  
* `forcedly_disable_remp_cap`: possible values `true` and `false`. The parameter is
  available only in `remp_emergency` compilation configuration. Allows to locally 
  disable REMP capability even if the capability is enabled by the network. May be
  used for network recovery.

* `remp_client_pool`: integer value 0 to 255. Number of threads (as a percentage of CPU Cores number), 
  used for preliminary message processing in REMP client.
  Default value is 100% (the number of threads equals the number of CPU Cores). 
  At least one thread is started anyway.

  Before being sent to validators, any external REMP message is executed in test mode on a client
  (proper blockchain state is constructed, virtual machine is activated etc), and if the message
  processing results in error, it is rejected on the client and not sent to validators.   