class RadonTranslator(object):
    def __init__(self):
        self.opcodes = (
            # array operations
            ("ArrayCount", 0x10),
            ("ArrayFilter", 0x11),
            ("ArrayGetArray", 0x13),
            ("ArrayGetBoolean", 0x14),
            ("ArrayGetBytes", 0x15),
            ("ArrayGetFloat", 0x16),
            ("ArrayGetInteger", 0x17),
            ("ArrayGetMap", 0x18),
            ("ArrayGetString", 0x19),
            ("ArrayMap", 0x1A),
            ("ArrayReduce", 0x1B),
            ("ArraySort", 0x1D),
            # boolean operations
            ("BooleanAsString", 0x20),
            ("BooleanNegate", 0x22),
            # byte operations
            ("BytesAsString", 0x30),
            ("BytesHash", 0x31),
            # integer operations
            ("IntegerAbsolute", 0x40),
            ("IntegerAsFloat", 0x41),
            ("IntegerAsString", 0x42),
            ("IntegerGreaterThan", 0x43),
            ("IntegerLessThan", 0x44),
            ("IntegerModulo", 0x46),
            ("IntegerMultiply", 0x47),
            ("IntegerNegate", 0x48),
            ("IntegerPower", 0x49),
            # float operations
            ("FloatAbsolute", 0x50),
            ("FloatAsString", 0x51),
            ("FloatCeiling", 0x52),
            ("FloatGreaterThan", 0x53),
            ("FloatFloor", 0x54),
            ("FloatLessThan", 0x55),
            ("FloatModulo", 0x56),
            ("FloatMultiply", 0x57),
            ("FloatNegate", 0x58),
            ("FloatPower", 0x59),
            ("FloatRound", 0x5B),
            ("FloatTruncate", 0x5D),
            # map operations
            ("MapGetArray", 0x61),
            ("MapGetBoolean", 0x62),
            ("MapGetBytes", 0x63),
            ("MapGetFloat", 0x64),
            ("MapGetInteger", 0x65),
            ("MapGetMap", 0x66),
            ("MapGetString", 0x67),
            ("MapKeys", 0x68),
            ("MapValues", 0x69),
            # string operations
            ("StringAsBoolean", 0x70),
            ("StringAsFloat", 0x72),
            ("StringAsInteger", 0x73),
            ("StringLength", 0x74),
            ("StringMatch", 0x75),
            ("StringParseJSONArray", 0x76),
            ("StringParseJSONMap", 0x77),
            ("StringToLowerCase", 0x79),
            ("StringToUpperCase", 0x7A),
        )

        self.filters = (
            ("GreaterThan", 0x00),
            ("LessThan", 0x01),
            ("Equals", 0x02),
            ("DeviationAbsolute", 0x03),
            ("DeviationRelative", 0x04),
            ("DeviationStandard", 0x05),
            ("Top", 0x06),
            ("Bottom", 0x07),
            ("Mode", 0x08),
            ("LessOrEqualThan", 0x80),
            ("GreaterOrEqualThan", 0x81),
            ("NotEquals", 0x82),
            ("NotDeviationAbsolute", 0x83),
            ("NotDeviationRelative", 0x84),
            ("NotDeviationStandard", 0x85),
            ("NotTop", 0x86),
            ("NotBottom", 0x87),
            ("NotMode", 0x88),
        )

        self.reducers = (
            ("Min", 0x00),
            ("Max", 0x01),
            ("Mode", 0x02),
            ("AverageMean", 0x03),
            ("AverageMeanWeighted", 0x04),
            ("AverageMedian", 0x05),
            ("AverageMedianWeighted", 0x06),
            ("DeviationStandard", 0x07),
            ("DeviationAverageAbsolute", 0x08),
            ("DeviationMedianAbsolute", 0x09),
            ("DeviationMaximumAbsolute", 0x0a),
            ("HashConcatenate", 0x0b),
        )

        self.error_codes = (
            ("Unknown", 0x00),
            ("SourceScriptNotCBOR", 0x01),
            ("SourceScriptNotArray", 0x02),
            ("SourceScriptNotRADON", 0x03),
            ("RequestTooManySources", 0x10),
            ("ScriptTooManyCalls", 0x11),
            ("UnsupportedOperator", 0x20),
            ("HTTPError", 0x30),
            ("RetrievalTimeout", 0x31),
            ("Underflow", 0x40),
            ("Overflow", 0x41),
            ("DivisionByZero", 0x42),
            ("NoReveals", 0x50),
            ("InsufficientConsensus", 0x51),
            ("InsufficientCommits", 0x52),
            ("TallyExecution", 0x53),
            ("MalformedReveal", 0x60),
            ("UnhandledIntercept", 0xFF),
        )

    def hex2str(self, hex_opc, hex_type):
        if hex_type == "opcode":
            lst = self.opcodes
        elif hex_type == "filter":
            lst = self.filters
        elif hex_type == "reducer":
            lst = self.reducers
        elif hex_type == "error":
            lst = self.error_codes
        else:
            return "Error"

        for opcode in lst:
            if hex_opc == opcode[1]:
                return opcode[0]

        return "Error"

    def str2hex(self, str_opc, str_type):
        if str_type == "opcode":
            lst = self.opcodes
        elif str_type == "filter":
            lst = self.filters
        elif str_type == "reducer":
            lst = self.reducers
        elif str_type == "error":
            lst = self.error_codes
        else:
            return 0xFF

        for opcode in lst:
            if str_opc == opcode[0]:
                return opcode[1]

        return 0xFF
