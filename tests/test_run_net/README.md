# How to run network emulation

Run test_run_net.sh to run validators and create network on local host:

```
bash ./test_run_net.sh
```

All required build procedures will be performed automatically. Then script will check that network is running normally and exits. Validator processes will remain running. You can check specific node status by its log:
                                                             
```
tail -f ./tmp/output_N.log      # N is the number of the node
```

Next run of this script will destroy previously running validators, clean up data and restart network anew.
                                                                   