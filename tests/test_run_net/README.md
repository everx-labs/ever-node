# How to run EverX blockchain emulation

Use following command to run nodes and create blockchain network on local host:

```
bash ./test_run_net.sh
```

All required build procedures will be performed automatically. Then script will ensure that network was started ok and exits. Node processes will remain running. There should be all validator nodes except node #0 which is not validaing. Next run of this script will destroy previously running nodes, clean up data and restart network anew.

It is possible to check specific node status by its log:
                                                             
```
tail -f ./tmp/output_N.log      # N is the number of the node, 
```

It is also possible to restart nodes (and rebuild if necessary):

```
bash ./restart_nodes.sh
```                                                                                                                                                                

Finally it is possible to kill all nodes and remove blockchain data:

```
bash ./stop_network.sh
```                                                                                                                                                                  
                                                                   