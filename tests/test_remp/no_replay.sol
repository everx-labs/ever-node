pragma ton-solidity >= 0.55.0;
//pragma AbiHeader time;

// This contract demonstrates custom replay protection functionality.
contract CustomReplaySample {

    uint public value;

    function addValue(uint num) public {
          tvm.accept();
          value += num;
    }

    function getValue() public view returns (uint) {
        return value;
    }

    // Function with predefined name which is used to replace custom replay protection.
    function afterSignatureCheck(TvmSlice body, TvmCell) private pure inline returns (TvmSlice) {
        // Via TvmSlice methods we read header fields from the message body

        body.decode(uint64); // The first 64 bits contain timestamp which is usually used to differentiate messages.
                
        // After reading message headers this function must return the rest of the body slice.
        return body;
    }
}