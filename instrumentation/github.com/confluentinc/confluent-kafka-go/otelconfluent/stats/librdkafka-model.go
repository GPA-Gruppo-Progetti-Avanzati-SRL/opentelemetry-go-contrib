package stats

type Model struct {
	Name             string             `json:"name"`
	ClientID         string             `json:"client_id"`
	Type             string             `json:"type"`
	Ts               int64              `json:"ts"`
	Time             int64              `json:"time"`
	Replyq           int64              `json:"replyq"`
	MsgCnt           int64              `json:"msg_cnt"`
	MsgSize          int64              `json:"msg_size"`
	MsgMax           int64              `json:"msg_max"`
	MsgSizeMax       int64              `json:"msg_size_max"`
	SimpleCnt        int64              `json:"simple_cnt"`
	MetadataCacheCnt int64              `json:"metadata_cache_cnt"`
	Brokers          map[string]Broker  `json:"brokers"`
	Topics           map[string]Topics  `json:"topics"`
	Cgpr             ConsumerGroupStats `json:"cgrp"`
	Tx               int64              `json:"tx"`
	TxBytes          int64              `json:"tx_bytes"`
	Rx               int64              `json:"rx"`
	RxBytes          int64              `json:"rx_bytes"`
	Txmsgs           int64              `json:"txmsgs"`
	TxmsgBytes       int64              `json:"txmsg_bytes"`
	Rxmsgs           int64              `json:"rxmsgs"`
	RxmsgBytes       int64              `json:"rxmsg_bytes"`
}

type ConsumerGroupStats struct {
	State          string `json:"state"`
	JoinState      string `json:"join_state"`
	RebalanceCnt   int64  `json:"rebalance_cnt"`
	AssignmentSize int64  `json:"assignment_size"`
	RebalanceAge   int64  `json:"rebalance_age"`
	StateAge       int64  `json:"stateage"`
}

type WindowStats struct {
	Min        int64 `json:"min"`
	Max        int64 `json:"max"`
	Avg        int64 `json:"avg"`
	Sum        int64 `json:"sum"`
	Stddev     int64 `json:"stddev"`
	P50        int64 `json:"p50"`
	P75        int64 `json:"p75"`
	P90        int64 `json:"p90"`
	P95        int64 `json:"p95"`
	P99        int64 `json:"p99"`
	P9999      int64 `json:"p99_99"`
	Outofrange int64 `json:"outofrange"`
	Hdrsize    int64 `json:"hdrsize"`
	Cnt        int64 `json:"cnt"`
}
type BrokerToppars struct {
	Topic     string `json:"topic"`
	Partition int64  `json:"partition"`
}

type Broker struct {
	Name           string        `json:"name"`
	Nodeid         int64         `json:"nodeid"`
	Nodename       string        `json:"nodename"`
	Source         string        `json:"source"`
	State          string        `json:"state"`
	Stateage       int64         `json:"stateage"`
	OutbufCnt      int64         `json:"outbuf_cnt"`
	OutbufMsgCnt   int64         `json:"outbuf_msg_cnt"`
	WaitrespCnt    int64         `json:"waitresp_cnt"`
	WaitrespMsgCnt int64         `json:"waitresp_msg_cnt"`
	Tx             int64         `json:"tx"`
	Txbytes        int64         `json:"txbytes"`
	Txerrs         int64         `json:"txerrs"`
	Txretries      int64         `json:"txretries"`
	ReqTimeouts    int64         `json:"req_timeouts"`
	Rx             int64         `json:"rx"`
	Rxbytes        int64         `json:"rxbytes"`
	Rxerrs         int64         `json:"rxerrs"`
	Rxcorriderrs   int64         `json:"rxcorriderrs"`
	Rxpartial      int64         `json:"rxpartial"`
	ZbufGrow       int64         `json:"zbuf_grow"`
	BufGrow        int64         `json:"buf_grow"`
	Wakeups        int64         `json:"wakeups"`
	Connect        int64         `json:"connects"`
	Disconnects    int64         `json:"disconnects"`
	IntLatency     WindowStats   `json:"int_latency"`
	Rtt            WindowStats   `json:"rtt"`
	Throttle       WindowStats   `json:"throttle"`
	Toppars        BrokerToppars `json:"toppars"`
}

type Partition struct {
	Partition       int64  `json:"partition"`
	Broker          int64  `json:"broker"`
	Leader          int64  `json:"leader"`
	Desired         bool   `json:"desired"`
	Unknown         bool   `json:"unknown"`
	MsgqCnt         int64  `json:"msgq_cnt"`
	MsgqBytes       int64  `json:"msgq_bytes"`
	XmitMsgqCnt     int64  `json:"xmit_msgq_cnt"`
	XmitMsgqBytes   int64  `json:"xmit_msgq_bytes"`
	FetchqCnt       int64  `json:"fetchq_cnt"`
	FetchqSize      int64  `json:"fetchq_size"`
	FetchState      string `json:"fetch_state"`
	QueryOffset     int64  `json:"query_offset"`
	NextOffset      int64  `json:"next_offset"`
	AppOffset       int64  `json:"app_offset"`
	StoredOffset    int64  `json:"stored_offset"`
	CommitedOffset  int64  `json:"commited_offset"`
	CommittedOffset int64  `json:"committed_offset"`
	EOFOffset       int64  `json:"eof_offset"`
	LoOffset        int64  `json:"lo_offset"`
	HiOffset        int64  `json:"hi_offset"`
	ConsumerLag     int64  `json:"consumer_lag"`
	Txmsgs          int64  `json:"txmsgs"`
	Txbytes         int64  `json:"txbytes"`
	Rxmsgs          int64  `json:"rxmsgs"`
	Rxbytes         int64  `json:"rxbytes"`
	Msgs            int64  `json:"msgs"`
	RxVerDrops      int64  `json:"rx_ver_drops"`
}

type Topics struct {
	Topic       string            `json:"topic"`
	MetadataAge int64             `json:"metadata_age"`
	Batchsize   WindowStats       `json:"batchsize"`
	Batchcnt    WindowStats       `json:"batchcnt"`
	Partitions  map[int]Partition `json:"partitions"`
}
