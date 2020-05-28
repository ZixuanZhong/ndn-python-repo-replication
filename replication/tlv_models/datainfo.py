from ndn.encoding import TlvModel, RepeatedField, BytesField, UintField

class DatainfoTlvModel(TlvModel):
    data_name = BytesField(0x02)        # string
    hash = BytesField(0x02)             # string
    desired_copies = UintField(0x03)    # int
