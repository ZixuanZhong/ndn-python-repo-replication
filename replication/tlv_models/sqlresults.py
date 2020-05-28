from ndn.encoding import TlvModel, RepeatedField, BytesField, UintField

class SqlresultsTlvModel(TlvModel):
    results = BytesField(0x02)  # serialized results in bytes