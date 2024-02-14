/*  */
pragma solidity >= 0.6.0;

import "Worker.sol";

contract Brazil is IBrazil {

    uint128 _counter = 0;
    uint128 _deployed = 0;
    uint128 _number = 10;
    TvmCell _contract2;
    TvmCell _code2; 
    TvmCell _data2;

    uint _key;
    uint128 _dgrant = 2e9;
    uint128 _giveMoney = 3e9;

    address _one;
    address _two;
    address _three;

    address[] _DeployedArray;
    address[] _WorkerHelps;

    modifier alwaysAccept {
	    tvm.accept(); 
	    _;
    }

    constructor() public alwaysAccept {
    }


    function setC(uint128 counter) public alwaysAccept {
	    if (counter < 0) {
               counter = 0;
            }
            _counter = counter;
	    while (_counter > _deployed){
	        deployWorker();
            }
            while (_deployed > _counter){
                stopWorker();
            }
    }

    function _isSameShard(address a1, address a2) private pure returns (bool) {   
            uint sh1 = a1.value >> 252;
	    uint sh2 = a2.value >> 252;
	    return (sh1 == sh2);
    }

    function deployWorker() public alwaysAccept {
            if (_DeployedArray.length > _counter){
            	while (_deployed < _counter){
                   Worker(_DeployedArray[_deployed]).start();
		   Worker(_WorkerHelps[_deployed]).start();
                   _deployed++;
                }
                return;
            } 
            if (_DeployedArray.length > _deployed){
               while (_deployed < _DeployedArray.length){
                   Worker(_DeployedArray[_deployed]).start();
                   Worker(_WorkerHelps[_deployed]).start();
                   _deployed++;
                }
                return;
            }
	    address a1;
	    address a2;
	    TvmCell s1;
	    TvmCell s2;

            _key = _key + rnd.next(3);	
	    s1 = tvm.insertPubkey(_contract2, _key);
	    a1 = address(tvm.hash(s1));

	    a2 = a1;
	    while (_isSameShard(a2, a1)) {
	        _key++;	
	        s2 = tvm.insertPubkey(_contract2, _key);
	        a2 = address(tvm.hash(s2));
	    }
            _DeployedArray.push(a1);
            _WorkerHelps.push(a2);
            _deployed++;
	    new Worker {stateInit:s1, value:_dgrant} (a2, _deployed);
	    new Worker {stateInit:s2, value:_dgrant} (a1, _deployed);
    }

    function stopWorker() public alwaysAccept {
           while (_deployed > _counter){
                Worker(_DeployedArray[_deployed - 1]).stop();
                Worker(_WorkerHelps[_deployed- 1]).stop();
                _deployed--;
           }
    }

    function NeedMoney(uint128 id) external override alwaysAccept {
//	    if (_DeployedArray[id - 1].balance() > 1e9){
//                return;
//            }
            if (id <= _deployed){
	        _DeployedArray[id - 1].transfer(_giveMoney);
                _WorkerHelps[id - 1].transfer(_giveMoney);
	    }
    }


    function build() public alwaysAccept {
	    _contract2 = tvm.buildStateInit(_code2, _data2);
    }

    function init() public alwaysAccept {
	    _counter = 0;
            _deployed = 0;
	    _key = rnd.next(3);
	    _contract2 = tvm.buildStateInit(_code2, _data2);
    }

    
    /* Setters */

    function setNumber(uint128 number) public alwaysAccept {
	    _number = number;
    }


    function setCode2(TvmCell c) public alwaysAccept {
	    _code2 = c;
    }

    function setData2(TvmCell c) public alwaysAccept {
	    _data2 = c;
    }

    function setContract2(TvmCell c) public alwaysAccept {
	    _contract2 = c;
    }

    function setKey(uint key) public alwaysAccept {
	    _key = key;
    }

    function setdGrant(uint128 dgrant) public alwaysAccept {
	    _dgrant = dgrant;
    }

    function setgMoney(uint128 money) public alwaysAccept {
            _giveMoney = money;
    }

    /* fallback/receive */
    receive() external {
	
    }

    function upgrade(TvmCell newcode) public {
//        require(msg.pubkey() == tvm.pubkey(), 101);
        tvm.accept();
	    tvm.commit();
        tvm.setcode(newcode);
        tvm.setCurrentCode(newcode);
//        onCodeUpgrade();
    }

    function grant(address addr, uint128 value) external {
        tvm.accept();
        addr.transfer(value, false, 3);
    }
}
