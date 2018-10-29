<?php
/**
 *  脚本模板
 */
Bd_Init::init();
ini_set('memory_limit', '2500M');

class Script{

    public function execute()
    {
        $column=array(
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
        );
        $productInfo=$this->getProductInfoAll();
        $out=array(
            "未找到快递记录"   => ['data'=>array(),'file_name'=>'未找到快递记录.csv'],
            "状态不一致"      => ['data'=>array(),'file_name'=>'状态不一致.csv'],
        );
        $objSendPool=new Service_Data_SendPool();
        $arrConds=array(
            'deleted' => 0,
            'status in ('.Service_Data_SendPool::STATUS_SENDOUT.",".Service_Data_SendPool::STATUS_RECEIVE.")",
        );

        $file_name="out.sql";
        $sql_update_sleep=1;
        for($index=100;$index<200;$index++){
            echo "index= ".$index."\n";
            $sendPoolData=$objSendPool->getSendListByConds($index,$arrConds,array());
            if($sendPoolData===false){
                echo "db error\n";
                return false;
            }
            echo "已寄送或已签收数据量：".count($sendPoolData)."\n";
            $express_numbers=array();
            foreach ($sendPoolData as $k=>$v){
                $express_numbers[]=$v['expressNumber'];
            }
            $expressStatus=$this->getAllSendStatus($express_numbers);

            foreach ($sendPoolData as $v){
                $status=$v['status'];
                $expressNumber=$v['expressNumber'];
                $v['partId']=$index;
                $v['status']=Service_Data_SendPool::$statusMap[$v['status']];
                $v['receiverPhone']=Wms_Util_Rc4::rc4Decode($v['receiverPhone']);
                $v['productName']=$productInfo[$v['productId']]['productName'];
                $v['createTime']=date('Y-m-d H:i:s',$v['createTime']);
                $v['updateTime']=date('Y-m-d H:i:s',$v['updateTime']);
                $v['sendTime']=date('Y-m-d H:i:s',$v['sendTime']);

                $es=array();
                if(!isset($expressStatus[$expressNumber])&&
                    !empty($expressNumber)&&
                    strpos($expressNumber,"VA")!==false){
                    $sendStatus=$this->getJingdongExpressSendStatus($expressNumber);

                    if(!empty($sendStatus)){
                        $es=$sendStatus;
                        $v['expressOrigin']="jingdongAPI";
                    }
                }else{
                    $es=$expressStatus[$expressNumber];
                    $v['expressOrigin']="tblExpressSendStatus";
                }

                if(empty($es)){
                    $out['未找到快递记录']['data'][]=$v;
                }else{
                    $v['sendStatus']=Service_Data_ExpressSendStatus::$arrStatusTransMap[$es['status']];
                    $v['statusDesc']=$es['statusDesc'];
                    if($status==Service_Data_SendPool::STATUS_SENDOUT){
                        if($es['status']==Service_Data_ExpressSendStatus::EXPRESS_SEND_STATUS_FLOW_FORWARD_FIN){
                            //5->6  从已寄出到已签收
                            $out['状态不一致']['data'][]=$v;
                            $sql="update tblSendPool".intval($v['parentOrderId']%100)." set status=".Service_Data_SendPool::STATUS_RECEIVE.
                                ",update_time=".time()." where id=".$v['id'].";";
                            $this->writeSqlToFile($file_name,$sql,$sql_update_sleep);
                        }elseif($es['status']==Service_Data_ExpressSendStatus::EXPRESS_SEND_STATUS_FLOW_BACKWARD_FIN||
                            $es['status']==Service_Data_ExpressSendStatus::EXPRESS_SEND_STATUS_FLOW_BACKWARD_SENDING){
                            //5->8  从已寄出到退签
                            $out['状态不一致']['data'][]=$v;
                            $sql="update tblSendPool".intval($v['parentOrderId']%100)." set status=".Service_Data_SendPool::STATUS_SENDFAIL.
                                ",update_time=".time()." where id=".$v['id'].";";
                            $this->writeSqlToFile($file_name,$sql,$sql_update_sleep);
                        }
                    }elseif($status==Service_Data_SendPool::STATUS_RECEIVE){
                        if($es['status']==Service_Data_ExpressSendStatus::EXPRESS_SEND_STATUS_FLOW_BACKWARD_FIN||
                            $es['status']==Service_Data_ExpressSendStatus::EXPRESS_SEND_STATUS_FLOW_BACKWARD_SENDING){
                            //6->8  从已签收到退签
                            $out['状态不一致']['data'][]=$v;
                            $sql="update tblSendPool".intval($v['parentOrderId']%100)." set status=".Service_Data_SendPool::STATUS_SENDFAIL.
                                ",update_time=".time()." where id=".$v['id'].";";
                            $this->writeSqlToFile($file_name,$sql,$sql_update_sleep);
                        }
                    }
                }
            }
        }
        foreach ($out as $k=>$v){
            $this->writeDataToFile($v['file_name'],$column,$v['data']);
        }

    }

    public function getProductInfoAll(){
        $objWms= Hk_Service_Db::getDB("zb/zb_wms");
        $sql_II="select product_id as productId,product_name as productName from tblInventoryInfo where deleted=0";
        $list_II=$objWms->query($sql_II);

        $out=array();
        foreach ($list_II as $v){
            $out[$v['productId']]['productId']=$v['productId'];
            $out[$v['productId']]['productName']=$v['productName'];
        }
        return $out;
    }

    public function writeSqlToFile($file_name,$content,&$index){
        if($index%2000==0){
            $sql="select sleep(2);";
            file_put_contents($file_name,$sql.PHP_EOL,FILE_APPEND);
        }
        file_put_contents($file_name,$content.PHP_EOL,FILE_APPEND);
        $index++;
    }

    public function getJingdongExpressSendStatus($expressNumber){
        $strJdRes = Wms_Thirdapi_JingdongApi::getJingdongExpressTrace($expressNumber);

        if ($strJdRes) {
            $arrJdRes      = json_decode($strJdRes, true);
            $arrSendStatus = Wms_Util_Jingdong::parseJingdongSendStatus($arrJdRes);
            return $arrSendStatus;
        }else{
            return false;
        }
    }

    public function getAllSendStatus($express_numbers){
        $objExpressSendStatus=new Service_Data_ExpressSendStatus();
        $sql_con=array();
        foreach ($express_numbers as $k=>$v){
            if(empty($v)){
                continue;
            }
            $num=intval($k%10);
            if(empty($sql_con[$num])){
                $sql_con[$num]="'".$v."'";
            }else{
                $sql_con[$num].=","."'".$v."'";
            }
        }

        $arrField=array(
            'expressNumber','status','statusDesc'
        );
        $ret=array();
        foreach ($sql_con as $sc){
            $arrConds=array(
                'deleted' => 0,
                "express_number in (".$sc.")",
            );
            $list=$objExpressSendStatus->getExpressSendStatusByConds($arrConds,$arrField);
            echo count($list)."\n";
            foreach ($list as $v){
                $ret[$v['expressNumber']]=$v;
            }
        }
        return $ret;
    }

    //取数据相关
    public function getWaitSendFromSendPoolTmp($arrField=array()){
        $objSendPoolTmp = new Service_Data_SendPoolTmp();

        $arrConds = array(
            'status' => 0,
            'deleted'   => 0,
        );
        $waitSendPoolList = $objSendPoolTmp->getNeedSendOrderListByConds($arrConds, $arrField);
        return $waitSendPoolList;
    }

    /**
     * 获取全量课程信息
     * @return array|bool
     */
    public function getCourseAll(){
        $objCourse  = Hk_Service_Db::getDB("fudao/miscourse");
        $LEARN_SEASON_MAP = array(
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

        //0、获取全量课程信息
        $sql_C="select course_id,course_name,grade_id,learn_season,start_time from tblCourse";
        $list_C=$objCourse->query($sql_C);
        if($list_C===false){
            Bd_log::warning("[db error] [sql:$sql_C]");
            echo "[db error] [sql:$sql_C]\n";
            return false;
        }
        $course=array();
        foreach ($list_C as $v){
            $course[$v['course_id']]['course_name']   = $v['course_name'];
            $course[$v['course_id']]['grade']         = $this->_gradeToXuebu($v['grade_id']);
            $course[$v['course_id']]['learn_season']  = $LEARN_SEASON_MAP[$v['learn_season']];
            $course[$v['course_id']]['start_time']    = date('Y-m-d H:i:s',$v['start_time']);
        }
        return $course;
    }

    /**
     * 获取全量商品的信息（包括商品具体信息和库存）
     * @param $productIds
     * @return array
     */
    public function getProductDetail($productIds){
        $objInventoryInfo = new Service_Data_InventoryInfo();
        $productList=$this->getProductDetailFromMisProduct($productIds);
        $inventoryInfo=$objInventoryInfo->getWaitSendInventoryListByConds($productIds, 1, array('productId','productName','inventoryCnt'));

        $inventory=array();
        foreach ($inventoryInfo as $v){
            $inventory[$v['productId']]=$v['inventoryCnt'];
        }
        $ret=array();
        foreach ($productList as $v){
            $ret[$v['productId']]['productId']=$v['productId'];
            $ret[$v['productId']]['productName']=$v['productName'];
            $ret[$v['productId']]['inventoryCnt']=isset($inventory[$v['productId']])?$inventory[$v['productId']]:0;
        }
        return $ret;
    }

    /**
     * 从商品后台获取商品信息
     * @param $pids
     * @return array|bool
     */
    private function getProductDetailFromMisProduct($pids) {
        //产品列表参数
        $arrParams   = array(
            'arrProductIds' => $pids,
        );
        $header = array(
            'pathinfo'     =>'/misproduct/interface/productlist',
            'querystring'  => http_build_query($arrParams),
        );

        $ret = ral("platmis", 'GET', array(), rand(100, 999), $header);

        if (false === $ret) {
            $errNo          = ral_get_errno();
            $errMsg         = ral_get_error();
            $protocolStatus = ral_get_protocol_code();
            Bd_Log::warning("Error:[service platmis connect error], Detail:[errno:$errNo errmsg:$errMsg protocol_status:$protocolStatus]");
            return false;
        }

        $data = json_decode($ret, true);

        $productData=array();
        if ($data['data']['productData'] && is_array($data['data']['productData'])){
            foreach($data['data']['productData'] as $item) {
                $productData[$item['productId']] = $item;
            }
            return $productData;
        }

        return array();
    }


    /**
     * 根据子订单号查询子订单信息（子订单号个数不能太多）
     * @param $new_sub_trade_ids
     * @return array|bool
     */
    public function getNewSubTradeByOrderIds($new_sub_trade_ids){
        $objDar = Hk_Service_Db::getDB("zb/zb_dar");
        //取出所有的子订单数据
        $sql_cons=$this->jointToSqlInCond($new_sub_trade_ids);
        $order_all=array();
        for ($index=0;$index<100;$index++){
            $sql="select user_id,sub_trade_id,trade_id,order_id,course_id,item_tag,history_data,trade_time from tblNewSubTrade".$index
                ." where sub_trade_id in (".$sql_cons.")";
            $list=$objDar->query($sql);
            if($list===false){
                echo "get new sub trade : db error\n";
                return false;
            }
            foreach ($list as $v){
                $history_data=json_decode($v['history_data'],true);
                $v['trade_record_id']           = $history_data['trade_record_id']?$history_data['trade_record_id']:0;
                $v['trade_record_parent_id']    = $history_data['trade_record_parent_id']?$history_data['trade_record_parent_id']:0;
                $v['trade_time']                = date('Y-m-d H:i:s',$v['trade_time']);
                $order_all[$v['sub_trade_id']]  = $v;
            }
        }
        return $order_all;
    }

    /**
     * 根据用户id和子订单id查询订单信息（可以查询大批量）
     * @param $new_sub_trade_ids
     * @return array|bool
     */
    public function getNewSubTradeByUserIdAndOrderIds($orders){
        $objDar = Hk_Service_Db::getDB("zb/zb_dar");
        //取出所有的子订单数据
        $sql_cons=array();
        foreach ($orders as $v){
            $sql_cons[intval($v['uid']%100)][$v['orderId']]=$v['orderId'];
        }

        $subTrades=array();
        foreach ($sql_cons as $k=>$v){
            $sc=$this->jointToSqlInCond($v);
            $sql="select user_id,sub_trade_id,trade_id,order_id,course_id,item_tag,history_data,trade_time from tblNewSubTrade".$k
                ." where sub_trade_id in (".$sc.")";
            $list=$objDar->query($sql);
            if($list===false){
                echo "get new sub trade : db error\n";
                return false;
            }
            foreach ($list as $sub_v){
                $history_data=json_decode($sub_v['history_data'],true);
                $sub_v['trade_record_id']           = $history_data['trade_record_id']?$history_data['trade_record_id']:0;
                $sub_v['trade_record_parent_id']    = $history_data['trade_record_parent_id']?$history_data['trade_record_parent_id']:0;
                $sub_v['trade_time']                = date('Y-m-d H:i:s',$sub_v['trade_time']);
                $subTrades[$sub_v['sub_trade_id']]  = $sub_v;
            }
        }
        return $subTrades;
    }


    /**
     * 获取班主任信息（post请求）
     * @param $studentUids
     * @param $courseIds
     * @return bool         返回 false 或者 $data[uid][courseid][]
     */
    public function getHeadTeacherData($studentUids, $courseIds){
        if(empty($studentUids)||!is_array($studentUids)){
            Bd_Log::warning("get head teacher error : param error studentUids[$studentUids]");
            return false;
        }
        if(empty($courseIds)||!is_array($courseIds)){
            Bd_Log::warning("get head teacher error : param error courseIds[$courseIds]");
            return false;
        }

        $header = array(
            'pathinfo'      => 'assistantdesk/api/getstudentcourse',
        );

        $arrParams = array(
            'studentUids'   => json_encode($studentUids),
            'courseIds'     => json_encode($courseIds),
        );

        $ret = ral('assistantdesk', 'POST', $arrParams, 123, $header);
        if (false === $ret) {
            $errno = ral_get_errno();
            $errmsg = ral_get_error();
            $protocol_status = ral_get_protocol_code();
            Bd_Log::warning("Error:[service tutormis connect error], Detail:[errno:$errno errmsg:$errmsg protocol_status:$protocol_status]");
            return false;
        }

        $ret = json_decode($ret, true);

        $errno = intval($ret['errNo']);
        $errmsg = strval($ret['errstr']);
        $headTeacherData=$ret['data'];

        if ($errno > 0) {
            Bd_Log::warning("Error:[service zb-miscourse process error], Detail:[errno:$errno errmsg:$errmsg]");
            return false;
        }else{
            return $headTeacherData;
        }
    }

    /**填充班主任信息
     * @param $data
     * @return bool
     */
    public function _fillHeadTeacherData(&$data){
        foreach ($data as $k=>$v){
            $headTeacherData=$this->getHeadTeacherData(array($v['student_uid']),array($v['course_id']));
            if($headTeacherData===false){
                echo "查询班主任信息失败\n";
                return false;
            }
            $headTeacher=$headTeacherData[$v['student_uid']][$v['course_id']];
            $data[$k]['head_teacher_uid']    = $headTeacher['assistantUid'];
            $data[$k]['head_teacher_name']   = $headTeacher['nickname'];
            $data[$k]['head_teacher_phone']  = $headTeacher['phone'];
        }
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

    /**
     * 将数据按照指定格式写入指定文件
     * @param $file_name        文件名
     * @param $column           文件表头及对应字段名
     * @param $data             数据
     * @param bool $is_append   是否是在后面新增，新增时不写入表头
     * @return bool
     */
    public function writeDataToFile($file_name,$column,$data,$is_append=false){
        if(empty($file_name)){
            Bd_Log::warning("[_writeDataToFail] param error");
            return false;
        }
        if(empty($column)){
            $column=array();
            foreach ($data[0] as $k=>$v){
                $column[]=['column_name'=>$k,'header_name'=>$k];
            }
        }

        //写入表头
        if(!$is_append){
            $title="";
            foreach ($column as $k=>$v){
                if($k==0){
                    $title.=$v['header_name'];
                }else{
                    $title.=",".$v['header_name'];
                }
            }
            $title = mb_convert_encoding($title,"GBK", "utf8");
            file_put_contents($file_name, $title . PHP_EOL);
        }

        //写入数据
        if(!empty($data)){
            foreach ($data as $line){
                $line_content="";
                foreach ($column as $k=>$v){
                    $cell_data = $line[$v['column_name']];
                    $cell_data = str_replace(',',';',$cell_data);
                    if($k==0){
                        $line_content.=$cell_data;
                    }else{
                        $line_content.=",".$cell_data;
                    }
                }
                $line_content = str_replace(array("\r","\n"),"",$line_content);
                $line_content = mb_convert_encoding($line_content,"GBK", "utf8");
                file_put_contents($file_name, $line_content . PHP_EOL,FILE_APPEND);
            }
        }
    }

    /**
     * 将数组的拼成字符串，用","隔开,其实就是implode函数
     * @param $data
     * @return string
     */
    public function jointToSqlInCond($data,$valueType=0){
        $sql_cond="";
        foreach ($data as $v){
            if(empty($v)){
                continue;
            }
            if($valueType==1){
                $v="'".$v."'";
            }
            if(empty($sql_cond)){
                $sql_cond=$v;
            }else{
                $sql_cond.=",".$v;
            }
        }
        return $sql_cond;
    }


    /**
     * 发送邮件
     * @param $receiver
     * @param $subject
     * @param $content
     * @param array $attach
     * @return bool
     */
    public function sendMail($receiver,$subject,$content,$attach=array()){
        if(!is_array($attach)){
            Bd_Log::warning("the param attach must be array");
            return false;
        }
        $ret=Hk_Util_Mail::sendMail($receiver, $subject, $content,$attach);
        if(!$ret){
            Bd_Log::warning("[Error] [send mail fail] [send mail result is:$ret]");
            return false;
        }
    }


    /**
     * 将年级转换成学部
     * 参考Hkzb_Const_FudaoGradeMap中的XUEQIAN，GAOZHONG，CHUZHONG，XIAOXUE
     * @param $grade
     * @return string
     */
    public function gradeToXuebu($grade){
        if($grade==0){
            return "";
        }
        if($grade&32768){
            return "学前";
        }
        if($grade&16384){
            return "高中";
        }
        if($grade&1024){
            return "初中";
        }
        if($grade&64){
            return "小学";
        }
        return "未知";
    }

}

echo "开始脚本\n";
$script=new Script();
$script->execute();
echo "结束脚本\n";



