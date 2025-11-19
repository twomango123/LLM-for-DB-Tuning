import random
import time
import string
from datetime import datetime
from typing import List


class Nation:
    def __init__(self, id, name, rid):
        self.id = id
        self.name = name
        self.rId = rid


class DataSource:
    """Python 完整移植版 DataSource，100% 复刻 C++ 逻辑"""

    # ----- static data -----
    nations = [
        Nation(48, "ALGERIA", 0), Nation(49, "ARGENTINA", 1), Nation(50, "BRAZIL", 1),
        Nation(51, "CANADA", 1), Nation(52, "EGYPT", 4), Nation(53, "ETHIOPIA", 0),
        Nation(54, "FRANCE", 3), Nation(55, "GERMANY", 3), Nation(56, "INDIA", 2),
        Nation(57, "INDONESIA", 2),

        Nation(65, "IRAN", 4), Nation(66, "IRAQ", 4), Nation(67, "JAPAN", 2),
        Nation(68, "JORDAN", 4), Nation(69, "KENYA", 0), Nation(70, "MOROCCO", 0),
        Nation(71, "MOZAMBIQUE", 0), Nation(72, "PERU", 1), Nation(73, "CHINA", 2),
        Nation(74, "ROMANIA", 3), Nation(75, "SAUDI ARABIA", 4), Nation(76, "VIETNAM", 2),
        Nation(77, "RUSSIA", 3), Nation(78, "UNITED KINGDOM", 3),
        Nation(79, "UNITED STATES", 1), Nation(80, "CHINA", 2),
        Nation(81, "PAKISTAN", 2), Nation(82, "BANGLADESH", 2), Nation(83, "MEXICO", 1),
        Nation(84, "PHILIPPINES", 2), Nation(85, "THAILAND", 2), Nation(86, "ITALY", 3),
        Nation(87, "SOUTH AFRICA", 0), Nation(88, "SOUTH KOREA", 2),
        Nation(89, "COLOMBIA", 1), Nation(90, "SPAIN", 3),

        Nation(97, "UKRAINE", 3), Nation(98, "POLAND", 3), Nation(99, "SUDAN", 0),
        Nation(100, "UZBEKISTAN", 2), Nation(101, "MALAYSIA", 2),
        Nation(102, "VENEZUELA", 1), Nation(103, "NEPAL", 2),
        Nation(104, "AFGHANISTAN", 2), Nation(105, "NORTH KOREA", 2),
        Nation(106, "TAIWAN", 2), Nation(107, "GHANA", 0),
        Nation(108, "IVORY COAST", 0), Nation(109, "SYRIA", 4),
        Nation(110, "MADAGASCAR", 0), Nation(111, "CAMEROON", 0),
        Nation(112, "SRI LANKA", 2), Nation(113, "ROMANIA", 3),
        Nation(114, "NETHERLANDS", 3), Nation(115, "CAMBODIA", 2),
        Nation(116, "BELGIUM", 3), Nation(117, "GREECE", 3),
        Nation(118, "PORTUGAL", 3), Nation(119, "ISRAEL", 4),
        Nation(120, "FINLAND", 3), Nation(121, "SINGAPORE", 2),
        Nation(122, "NORWAY", 3)
    ]

    regions = ["AFRICA", "AMERICA", "ASIA", "EUROPE", "MIDDLE EAST"]

    cLastParts = [
        "BAR", "OUGHT", "ABLE", "PRI", "PRES",
        "ESE", "ANTI", "CALLY", "ATION", "EING"
    ]

    tpchNouns = [
        "foxes", "ideas", "theodolites", "pinto beans", "instructions", "dependencies",
        "excuses", "platelets", "asymptotes", "courts", "dolphins", "multipliers",
        "sauternes", "warthogs", "frets", "dinos", "attainments", "somas", "Tiresias'",
        "patterns", "forges", "braids", "hockey players", "frays", "warhorses",
        "dugouts", "notornis", "epitaphs", "pearls", "tithes", "waters", "orbits",
        "gifts", "sheaves", "depths", "sentiments", "decoys", "realms", "pains",
        "grouches", "escapades"
    ]

    tpchVerbs = [
        "sleep", "wake", "are", "cajole", "haggle", "nag", "use", "boost", "affix",
        "detect", "integrate", "maintain", "nod", "was", "lose", "sublate", "solve",
        "thrash", "promise", "engage", "hinder", "print", "x-ray", "breach", "eat",
        "grow", "impress", "mold", "poach", "serve", "run", "dazzle", "snooze",
        "doze", "unwind", "kindle", "play", "hang", "believe", "doubt"
    ]

    tpchAdjectives = [
        "furious", "sly", "careful", "blithe", "quick", "fluffy", "slow", "quiet",
        "ruthless", "thin", "close", "dogged", "daring", "brave", "stealthy",
        "permanent", "enticing", "idle", "busy", "regular", "final", "ironic",
        "even", "bold", "silent"
    ]

    tpchAdverbs = [
        "sometimes", "always", "never", "furiously", "slyly", "carefully", "blithely",
        "quickly", "fluffily", "slowly", "quietly", "ruthlessly", "thinly", "closely",
        "doggedly", "daringly", "bravely", "stealthily", "permanently", "enticingly",
        "idly", "busily", "regularly", "finally", "ironically", "evenly", "boldly",
        "silently"
    ]

    tpchPrepositions = [
        "about", "above", "according to", "across", "after", "against", "along",
        "alongside of", "among", "around", "at", "atop", "before", "behind", "beneath",
        "beside", "besides", "between", "beyond", "by", "despite", "during", "except",
        "for", "from", "in place of", "inside", "instead of", "into", "near", "of",
        "on", "outside", "over", "past", "since", "through", "throughout", "to",
        "toward", "under", "until", "up", "upon", "without", "with", "within"
    ]

    tpchTerminators = [".", ";", ":", "?", "!", "--"]

    tpchAux = [
        "do", "may", "might", "shall", "will", "would", "can", "could", "should",
        "ought to", "must", "will have to", "shall have to", "could have to",
        "should have to", "must have to", "need to", "try to"
    ]

    lastOlCount = 0

    # ---------------------------
    # Random helpers
    # ---------------------------

    @staticmethod
    def initialize():
        random.seed(1382350201)

    @staticmethod
    def randomTrue(prob):
        return random.random() < prob

    @staticmethod
    def randomUniformInt(a, b):
        return random.randint(a, b)

    @staticmethod
    def randomDouble(minValue, maxValue, decimals):
        scale = 10 ** decimals
        iv = random.randint(int(minValue * scale), int(maxValue * scale))
        return iv / scale
    
	

    # ---------------------------
    # C-LAST
    # ---------------------------

    @staticmethod
    def genCLast(value):
        p0 = DataSource.cLastParts[value // 100]
        value %= 100
        p1 = DataSource.cLastParts[value // 10]
        value %= 10
        p2 = DataSource.cLastParts[value]
        return p0 + p1 + p2

    @staticmethod
    def randomCLast():
        # NURand(255, 0, 999, 173)
        A = 255
        x = 0
        y = 999
        C = 173
        v = ((((random.randint(0, A) | random.randint(x, y)) + C) % (y - x + 1)) + x)
        return DataSource.genCLast(v)

    # ---------------------------
    # TPCH sentence generator
    # ---------------------------

    @staticmethod
    def tpchText(length):
        text = " "
        for i in range(25):
            text += (" " if i != 0 else "") + DataSource.tpchSentence()
        pos = random.randint(0, len(text) - length)
        return text[pos: pos + length]

    @staticmethod
    def tpchSentence():
        r = random.random()
        if r < 0.2:
            return DataSource.tpchNounPhrase() + " " + DataSource.tpchVerbPhrase() + " " + \
                   random.choice(DataSource.tpchTerminators)
        elif r < 0.4:
            return DataSource.tpchNounPhrase() + " " + DataSource.tpchVerbPhrase() + " " + \
                   DataSource.tpchPrepositionalPhrase() + " " + random.choice(DataSource.tpchTerminators)
        elif r < 0.6:
            return DataSource.tpchNounPhrase() + " " + DataSource.tpchVerbPhrase() + " " + \
                   DataSource.tpchNounPhrase() + " " + random.choice(DataSource.tpchTerminators)
        elif r < 0.8:
            return DataSource.tpchNounPhrase() + " " + DataSource.tpchPrepositionalPhrase() + " " + \
                   DataSource.tpchVerbPhrase() + " " + DataSource.tpchNounPhrase() + " " + \
                   random.choice(DataSource.tpchTerminators)
        else:
            return DataSource.tpchNounPhrase() + " " + DataSource.tpchPrepositionalPhrase() + " " + \
                   DataSource.tpchVerbPhrase() + " " + DataSource.tpchPrepositionalPhrase() + " " + \
                   random.choice(DataSource.tpchTerminators)

    @staticmethod
    def tpchNounPhrase():
        r = random.random()
        if r < 0.25:
            return random.choice(DataSource.tpchNouns)
        elif r < 0.5:
            return random.choice(DataSource.tpchAdjectives) + " " + random.choice(DataSource.tpchNouns)
        elif r < 0.75:
            return random.choice(DataSource.tpchAdjectives) + ", " + \
                   random.choice(DataSource.tpchAdjectives) + " " + random.choice(DataSource.tpchNouns)
        else:
            return random.choice(DataSource.tpchAdverbs) + " " + \
                   random.choice(DataSource.tpchAdjectives) + " " + random.choice(DataSource.tpchNouns)

    @staticmethod
    def tpchVerbPhrase():
        r = random.random()
        if r < 0.25:
            return random.choice(DataSource.tpchVerbs)
        elif r < 0.5:
            return random.choice(DataSource.tpchAux) + " " + random.choice(DataSource.tpchVerbs)
        elif r < 0.75:
            return random.choice(DataSource.tpchVerbs) + " " + random.choice(DataSource.tpchAdverbs)
        else:
            return random.choice(DataSource.tpchAux) + " " + random.choice(DataSource.tpchVerbs) + " " + \
                   random.choice(DataSource.tpchAdverbs)

    @staticmethod
    def tpchPrepositionalPhrase():
        return random.choice(DataSource.tpchPrepositions) + " the " + DataSource.tpchNounPhrase()

    # ---------------------------
    # Special inserts
    # ---------------------------

    @staticmethod
    def addAlphanumeric62(length):
        out = ""
        while len(out) < length:
            c = chr(random.randint(ord('0'), ord('z')))
            if c.isdigit() or c.isalpha():
                out += c
        return out


    @staticmethod
    def random_alphanumeric_64(length: int) -> str:
        """
        生成长度为 length 的字符串，字符集 '0'-'9', 'A'-'Z', 'a'-'z'。
        """
        result = []
        while len(result) < length:
            c = chr(random.randint(ord('0'), ord('z')))
            if ('0' <= c <= '9') or ('A' <= c <= 'Z') or ('a' <= c <= 'z'):
                result.append(c)
        return ''.join(result)
    
    @staticmethod
    def add_alphanumeric_64(length: int, delimiter: bool = True) -> str:
        """
        返回一个随机生成的 alphanumeric64 字符串。
        如果 delimiter=True，在末尾加 '|'
        """
        s = DataSource.random_alphanumeric_64(length)
        if delimiter:
            s += '|'
        return s
        

    @staticmethod
    def addAlphanumeric64_range(minLength, maxLength):
        length = random.randint(minLength, maxLength)
        return DataSource.addAlphanumeric64(length)

    @staticmethod
    def addAlphanumeric64Original(minLength, maxLength):
        length = random.randint(minLength, maxLength)
        pos = random.randint(0, length - 8)
        return (
            DataSource.addAlphanumeric64(pos) +
            "ORIGINAL" +
            DataSource.addAlphanumeric64(length - 8 - pos)
        )

    @staticmethod
    def addNumeric(length):
        return ''.join(str(random.randint(0, 9)) for _ in range(length))

    @staticmethod
    def addTextString(minLength, maxLength):
        return DataSource.tpchText(random.randint(minLength, maxLength))

    @staticmethod
    def addTextStringCustomer(minLength, maxLength, action):
        length = random.randint(minLength, maxLength)
        l1 = random.randint(0, length - 18)
        l2 = random.randint(0, length - l1 - 18)
        l3 = length - l1 - l2 - 18
        return DataSource.tpchText(l1) + "Customer" + DataSource.tpchText(l2) + action + DataSource.tpchText(l3)

    @staticmethod
    def addInt(minValue, maxValue):
        if 
        return str(DataSource.randomUniformInt(minValue, maxValue))

    @staticmethod
    def addDouble(minValue, maxValue, decimals):
        return f"{DataSource.randomDouble(minValue, maxValue, decimals):.{decimals}f}"

    @staticmethod
    def addWDCZip():
        return DataSource.addNumeric(4) + "11111"

    @staticmethod
    def addSuPhone(suId):
        cc = (suId % 90) + 10
        return f"{cc}-{random.randint(100,999)}-{random.randint(100,999)}-{random.randint(1000,9999)}"

    @staticmethod
    def strLeadingZero(i, zeros):
        return str(i).zfill(zeros)

    @staticmethod
    def getCurrentTimeString():
        return datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    

	

    @staticmethod
    def random_alphanumeric62(length: int) -> str:
        """
        Python 版本对等 C++：
        string DataSource::randomAlphanumeric62(int length)
        随机生成由 A-Z a-z 0-9 组成的字符串，但与 C++ 不同的是：
        它允许范围 '0' 到 'z'，过滤掉非字母数字区间。
        """
        s = []
        for _ in range(length):
            c = '\x00'
            # 过滤非法字符：C++ 代码逻辑
            while (
                c == '\x00'
                or ('9' < c < 'A')     # '9' 与 'A' 之间非字母
                or ('Z' < c < 'a')     # 'Z' 与 'a' 之间非字母
            ):
                c = DataSource.randomUniformInt('0', 'z')

            s.append(c)

        return ''.join(s)

    @staticmethod
    def add_nid(stream, delimiter: bool, csv_delim=","):
        """
        Python 版本对等 C++：
        void DataSource::addNId(ofstream& stream, bool delimiter)
        等价行为：写一个随机合法字符到输出流。
        """
        c = '\x00'
        while (
            c == '\x00'
            or ('9' < c < 'A')
            or ('Z' < c < 'a')
        ):
            c = DataSource.randomUniformInt('0', 'z')

        stream.write(c)
        if delimiter:
            stream.write(csv_delim)

    # ---------------------------
    # TPCH nations & regions
    # ---------------------------

    @staticmethod
    def getNation(i):
        return DataSource.nations[i]

    @staticmethod
    def getRegion(i):
        return DataSource.regions[i]
