<?php
/**
 *  脚本模板
 */
Bd_Init::init();
ini_set('memory_limit', '2500M');

class Script{
    private $_objSendPool;
    private $_objSendPoolTmp;
    private $_objInventoryInfo;
    private $_objexpressSendStatus;
    //收件人信息
    private $receivers_send;
    private $receivers_stockout;

    public static $LEARN_SEASON_MAP = array(
        11 => '春一期',
        12 => '春二期',
        13 => '春三期',
        14 => '春四期',
        20 => '暑零期',
        21 => '暑一期',
        22 => '暑二期',
        23 => '暑三期',
        24 => '暑四期',
        25 => '暑五期',
        26 => '暑六期',
        31 => '秋一期',
        32 => '秋二期',
        33 => '秋三期',
        34 => '秋四期',
        41 => '寒一期',
        42 => '寒二期',
        43 => '寒三期',
        44 => '寒四期',
        1 => '春',
        2 => '暑',
        3 => '秋',
        4 => '寒',
    );

    public function __construct(){
        $this->_objSendPool         = new Service_Data_SendPool();
        $this->_objSendPoolTmp      = new Service_Data_SendPoolTmp();
        $this->_objInventoryInfo    = new Service_Data_InventoryInfo();
        $this->_objexpressSendStatus= new Service_Data_ExpressSendStatus();

        //设置收件
        $test=0;
        switch ($test){
            case 0:
                //测试
                $this->receivers_send="raozeshu@zuoyebang.com";
                $this->receivers_stockout=$this->receivers_send;
                break;
            case 1:
                //内部测试
                $this->receivers_send="raozeshu@zuoyebang.com,fangqun@zuoyebang.com,luguifu@zuoyebang.com,".
                    "liangjian@zuoyebang.com,zhuyaohui@zuoyebang.com,renjie01@zuoyebang.com,".
                    "tangpan@zuoyebang.com,xianglizhao@zuoyebang.com,liang@zuoyebang.com";
                $this->receivers_stockout=$this->receivers_send;
                break;
            case 2:
                //正式
                $this->receivers_send=
                    "chengongming@zuoyebang.com,liutao02@zuoyebang.com,".
                    "fuyizheng@zuoyebang.com,chengzixia@zuoyebang.com,".
                    "liangshuang@zuoyebang.com,hongdingqian@zuoyebang.com,".
                    "mashuai01@zuoyebang.com,liang@zuoyebang.com,".
                    "huangxin01@zuoyebang.com,luguifu@zuoyebang.com,".
                    "fangqun@zuoyebang.com,liangjian@zuoyebang.com,".
                    "zhuyaohui@zuoyebang.com,wangjiaqi@zuoyebang.com,".
                    "houshengnan@zuoyebang.com,shijinyu@zuoyebang.com,".
                    "linbin@zuoyebang.com,zhaohuijuan@zuoyebang.com,".
                    "panlijun@zuoyebang.com,raozeshu@zuoyebang.com";
                $this->receivers_stockout=
                    "liuguiping@zuoyebang.com,chengongming@zuoyebang.com,".
                    "liutao02@zuoyebang.com,fuyizheng@zuoyebang.com,".
                    "chengzixia@zuoyebang.com,liangshuang@zuoyebang.com,".
                    "hongdingqian@zuoyebang.com,mashuai01@zuoyebang.com,".
                    "liang@zuoyebang.com,huangxin01@zuoyebang.com,".
                    "luguifu@zuoyebang.com,fangqun@zuoyebang.com,".
                    "liangjian@zuoyebang.com,zhuyaohui@zuoyebang.com,".
                    "houshengnan@zuoyebang.com,shijinyu@zuoyebang.com,".
                    "raozeshu@zuoyebang.com";
                break;
        }
    }

    public function execute()
    {
        $ret=$this->clearData();
        if($ret===false){
            echo "failed \n";
            return;
        }
    }


    //region ----------------清理补寄回传错误数据----------------
    public function clearData(){
        //csv数据格式
        $column=[
            ["column_name"=>"partId"                , "header_name"=>"分表id"],
            ["column_name"=>"id"                    , "header_name"=>"id"],
            ["column_name"=>"parentOrderId"         , "header_name"=>"父订单号"],
            ["column_name"=>"orderId"               , "header_name"=>"订单号"],
            ["column_name"=>"uid"                   , "header_name"=>"用户uid"],
            ["column_name"=>"productId"             , "header_name"=>"产品id" ],
            ["column_name"=>"productName"           , "header_name"=>"产品名称" ],
            ["column_name"=>"receiverName"          , "header_name"=>"用户姓名"],
            ["column_name"=>"receiverPhone"         , "header_name"=>"收件人手机"],
            ["column_name"=>"receiverAddress"       , "header_name"=>"收件人地址"],
            ["column_name"=>"createTime"            , "header_name"=>"创建时间"],
            ["column_name"=>"updateTime"            , "header_name"=>"更新时间"],
            ["column_name"=>"sendTime"              , "header_name"=>"寄送时间"],
            ["column_name"=>"status"                , "header_name"=>"状态"],
            ["column_name"=>"expressNumber"         , "header_name"=>"快递单号"],
            ["column_name"=>"sendStatus"            , "header_name"=>"快递状态"],
            ["column_name"=>"statusDesc"            , "header_name"=>"状态描述"],
            ["column_name"=>"expressOrigin"         , "header_name"=>"快递数据来源"],
        ];
        $file="./updateStatus1029.sql";
        $data=$this->getCsvData('input.csv',$column);
        var_dump($data[0]);
        $index=0;
        $statistics=array();
        foreach ($data as $v){
            if($index%2000==0){
                echo $index."\n";
            }
            $status=$v['status'];
            $sendStatus=$v['sendStatus'];

            if($status==Service_Data_SendPool::$statusMap[Service_Data_SendPool::STATUS_SENDOUT]){
                $status=Service_Data_SendPool::STATUS_SENDOUT;
            }elseif($status==Service_Data_SendPool::$statusMap[Service_Data_SendPool::STATUS_RECEIVE]){
                $status=Service_Data_SendPool::STATUS_RECEIVE;
            }else{
                echo "数据源错误\n";
                return false;
            }

            $statusUpdate=-1;
            if($sendStatus==Service_Data_ExpressSendStatus::$arrStatusTransMap[Service_Data_ExpressSendStatus::EXPRESS_SEND_STATUS_FLOW_FORWARD_FIN]){
                $statusUpdate=Service_Data_SendPool::STATUS_RECEIVE;
            }elseif($sendStatus==Service_Data_ExpressSendStatus::$arrStatusTransMap[Service_Data_ExpressSendStatus::EXPRESS_SEND_STATUS_FLOW_BACKWARD_SENDING]||
                $sendStatus==Service_Data_ExpressSendStatus::$arrStatusTransMap[Service_Data_ExpressSendStatus::EXPRESS_SEND_STATUS_FLOW_BACKWARD_FIN]){
                $statusUpdate=Service_Data_SendPool::STATUS_SENDFAIL;
            }

            if(($statusUpdate==6&&$status==5)||($statusUpdate==8&&($status==5||$status==6))){
                $sql="update tblSendPool".intval($v['parentOrderId']%100)." set status=".$statusUpdate.
                    ",update_time=".time()." where id=".$v['id']." and status=".$status.";";
                file_put_contents($file,$sql.PHP_EOL,FILE_APPEND);
                $index++;
                if($index%2000==0){
                    $sql="select sleep(2);";
                    file_put_contents($file,$sql.PHP_EOL,FILE_APPEND);
                }
                $statistics[$status.'_'.$statusUpdate]++;
            }else{
                echo "数据源错误:$status->$statusUpdate\n";
                return false;
            }
        }
        var_dump($statistics);
    }

    /**
     * 读取CSV文件的内容
     * @param $file_name
     * @param $column
     * @param bool $is_has_header
     * @return array
     */
    public function getCsvData($file_name,$column,$is_has_header=true){
        //读取文件内容
        $file=file_get_contents($file_name);
        $lines=explode(PHP_EOL,$file);
        $data=array();
        foreach ($lines as $k=>$line){
            //去除表头
            if($k==0&&$is_has_header){
                continue;
            }
            if(empty($line)){
                continue;
            }
            //处理每一行的数据
            $cells=explode(",",$line);
            if(!empty($cells)){
                $line_data=array();
                foreach ($cells as $sub_k=>$sub_v) {
                    $line_data[$column[$sub_k]['column_name']]=$sub_v;
                }
                $data[]=$line_data;
            }
        }
        return $data;
    }
    //endregion

}

echo "开始脚本\n";
$script=new Script();
$script->execute();
echo "结束脚本\n";



