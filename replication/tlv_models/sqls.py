from ndn.encoding import TlvModel, RepeatedField, BytesField, UintField

class SqlsTlvModel(TlvModel):
    sqls = RepeatedField(BytesField(0x02)) # multiple SQL statements, each is a byte array