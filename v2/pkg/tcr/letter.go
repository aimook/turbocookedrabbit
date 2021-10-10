package tcr

import (
	"github.com/google/uuid"
	"github.com/streadway/amqp"
)

// Letter contains the message body and address of where things are going.
type Letter struct {
	LetterID   uuid.UUID
	RetryCount uint32
	Body       []byte
	Envelope   *Envelope
}

// Envelope contains all the address details of where a letter is going. 发送消息内容封装
type Envelope struct {
	Exchange     string
	RoutingKey   string
	ContentType  string
	Mandatory    bool
	Immediate    bool
	Headers      amqp.Table
	DeliveryMode uint8
	Priority     uint8
}

// WrappedBody is to go inside a Letter struct with indications of the body of data being modified (ex., compressed).
type WrappedBody struct {
	LetterID       uuid.UUID   `json:"LetterID"`
	Body           *ModdedBody `json:"Body"`
	LetterMetadata string      `json:"LetterMetadata"`
}

// ModdedBody is a payload with modifications and indicators of what was modified. 消息载荷封装
type ModdedBody struct {
	Encrypted   bool   `json:"Encrypted"`                 //数据是否加密
	EType       string `json:"EncryptionType,omitempty"`  //加密类型
	Compressed  bool   `json:"Compressed"`                //是否启用压缩
	CType       string `json:"CompressionType,omitempty"` //数据压缩类型
	UTCDateTime string `json:"UTCDateTime"`               //数据时间信息
	Data        []byte `json:"Data"`                      //数据内容
}
