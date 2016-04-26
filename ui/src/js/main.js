/*	alarmOperation部分 */
function alarmSetting() {
    var queueName = getNameSelections();
    var alarm_target = $('#alarm_target').val();
    var message_accumulation = $('#message_accumulation').val();
    var message_delay = $('#message_delay').val();
    var message_producer_num = $('message_producer_num').val();

    toastr.success("设定成功");

    $('#alarmSetting').modal('hide');
    $('#alarm_target').val("");
    $('#message_accumulation').val("");
}

function lookupAlarmInfo(queueName) {
    var alarmDiv = document.getElementById("alarmDetailInfo");
    var table = document.createElement("table");
    table.setAttribute("BORDER-COLLAPSE", "collapse");
    table.setAttribute("cellPadding", "1");
    table.setAttribute("width", "800");
    table.setAttribute("height", "100");
    table.setAttribute("border", "0");
    table.setAttribute("align", "center");
    createInfoRow("报警邮件发送人", "yangpeng3,menglong,ruihong,sifang", null, null, table);
    createInfoRow("堆积阈值", "10000", null, null, table);
    alarmDiv.appendChild(table);
}

function createTable(data) {
    var qname = data[0].queue;
    var logSize = data[0].length; //可能会有问题
    var timestamp = data[0].ctime;
    var biz = data[0].groups[0].group;
    var write = data[0].groups[0].write;
    var read = data[0].groups[0].read;
    var url = data[0].groups[0].url;
    var sendCount = data[0].groups[0].sendCount;
    var receiveCount = data[0].groups[0].receiveCount;
    var d = new Date(timestamp * 1000);    //根据时间戳生成的时间对象
    var ctime = (d.getFullYear()) + "-" +
           (d.getMonth() + 1) + "-" +
           (d.getDate()) + " " +
           (d.getHours()) + ":" +
           (d.getMinutes()) + ":" +
           (d.getSeconds());
    var alarmReceiver = "sifang,menglong,ruihong1,yangpeng3";
    var messageAccumulation = "10000";
    var messageDelay = "500";
    var messageDelayProducerNum = "1000";
    var jsonstr = '[{firstCell:"队列名称："+qname,secondCell:"接入方："+biz},'
                  +'{firstCell:"创建时间："+ctime,secondCell:"日志大小："+logSize},'
                  +'{firstCell:"接入方写权限："+write,secondCell:"接入方读权限："+read},'
                  +'{firstCell:"写入量："+receiveCount,secondCell:"读取量："+sendCount},'
                  +'{firstCell:"url："+url,secondCell:""},'
                  +'{firstCell:"报警邮件接收人："+alarmReceiver,secondCell:"消息堆积阈值："+messageAccumulation},'
                  +'{firstCell:"消息延迟阈值："+messageDelay,secondCell:"消息写入量阈值："+messageDelayProducerNum}]';
    var jsonarray = eval('('+jsonstr+')');
    $('#bizInfo').bootstrapTable({
        classes:'table',
        showHeader: false,
            columns: [
            {
                field: 'firstCell',
                align: 'left'
            }, {
                field: 'secondCell',
                align: 'left'
            }],
            data: jsonarray,   //data数据类型应该json数组，不能是json字符串
        });
}

function createTableMultiChoice() {
    var jsonstr = '[{firstCell:"只能同时选中一个队列业务方来进行详细信息展示"}]';
    var jsonarray = eval('('+jsonstr+')');
    $('#bizInfo').bootstrapTable({
        classes:'table',
        showHeader: false,
            columns: [
            {
                field: 'firstCell',
                align: 'left'
            }],
            data: jsonarray,   //data数据类型应该json数组，不能是json字符串
    });
}

function clearDisplayInfo() {
    $('#bizInfo').bootstrapTable('destroy');
}

function clearZoomDiv() {
    clearDisplayInfo();
    chosenBizMap.clear();
    destoryMonitor();
}

/*	bizOperation部分*/
function addBiz() {
    var queueName = getNameSelections();
    for (var i=0;i<queueName.length;i++) {
        var qname = queueName[i];
        var biz = $('#bizName').val();
        var write = $('#writeOperation').val();
        var read = $('#readOperation').val();
        $.ajax({
        url:"/group",
        type: "POST",
        dataType:'json',
        data: "action=add&queue="+qname+"&group="+biz+"&write="+write+"&read="+read,
        success:function(msg) {
            if (msg.result == true) {
                toastr.success("添加成功!");
                $table.bootstrapTable('refresh');
                $queueAction.prop('disabled',true);
                clearZoomDiv();
            }else{
                toastr.error("添加失败!");
            }
        },
        error: function () {
            toastr.error("添加失败!");
        }
        });
    }
    $('#addBizs').modal('hide');
    $('#bizName').val("");
    $('#writeOperation').val("false");
    $('#readOperation').val("false");
}

function deleteBiz() {
    $('#delBizModal').modal("show");
    $('#realDeleteBiz').unbind('click');
    $('#realDeleteBiz').bind('click',function(){
        var queueName = getNameSelections();
        for(var i=0;i<queueName.length;i++) {
            var qname = queueName[i];
            var $subTable = $('#bizTable_'+qname);
            var bizs = getSubTableSelections($subTable);
            console.info(bizs);
            if(bizs.length == 0) {
                alert("opps,没有选择业务方哦！");
                return;
            }
            for (var j=0;j<bizs.length;j++) {
                var biz = bizs[j];
                $.ajax({
                    url:"/group",
                    type: "POST",
                    dataType:'json',
                    data: "action=remove&queue="+qname+"&group="+biz,
                    success:function(msg) {
                        if (msg.result == true) {
                            toastr.success("删除成功!");
                            $table.bootstrapTable('refresh');
                            $queueAction.prop('disabled',true);
                            clearZoomDiv();
                        }else{
                            toastr.error("删除失败!");
                        }
                    },
                    error: function () {
                        toastr.error("删除失败!");
                    }
                });
            }
            $subTable.bootstrapTable('remove', {
                field: 'group',
                values: bizs
            });
    }
    });
}

function modifyBiz() {
    var queueName = getNameSelections();
    for(var i=0;i<queueName.length;i++) {
        var qname = queueName[i];
        var $subTable = $('#bizTable_'+qname);
        var bizs = getSubTableSelections($subTable);
        if(bizs.length==0) {
            alert("opps,队列"+qname+"没有选择业务方哦！");
            return;
        }
        for (var j=0;j<bizs.length;j++) {
            var biz = bizs[j];
            var write = $('#writeOperation_modify').val();
            var read = $('#readOperation_modify').val();
            $.ajax({
                url:"/group",
                type: "POST",
                dataType:'json',
                data: "action=update&queue="+qname+"&group="+biz+"&write="+write+"&read="+read,
                success:function(msg) {
                    if (msg.result == true) {
                        toastr.success("更新成功!");
                        $table.bootstrapTable('refresh');
                        $queueAction.prop('disabled',true);
                        clearZoomDiv();
                    }else{
                        toastr.error("更新失败!");
                    }
                },
                error: function () {
                    toastr.error("更新失败!");
                }
            });
        }
    }
    $('#modifyBizs').modal('hide');
    $('#writeOperation_modify').val("false");
    $('#readOperation_modify').val("false");

}

function searchBiz(qname,biz) {
    if (biz == null) {
        alert("opps,没有选择业务方哦！");
    }
    $.ajax({
        url:"/group",
        type: "get",
        dataType:'json',
        data: "action=lookup&group="+biz,
        success:function(msg) {
            if(msg != null){
                if (msg[0].group == biz) {
                    var result = msg[0].queues;
                    createTable(result);
                }
            }
        },
        error: function () {
            toastr.error("获取失败!");
        }
    });
}

function getSubTableSelections($table) {
    return $.map($table.bootstrapTable('getSelections'), function (row) {
        return row.group
    });
}
/*	footer部分 */
var zoomDiv=document.getElementById("footer");

/*	monitor部分 */
var legendData = [];
var readResult = [];
var legendWriteData = [];
var writeResult = [];
var Qname ;
var Biz;
var myChart;
var myChart2;
function createOption() {
    return option = {
        tooltip:{
            trigger:'axis'
        },
        legend:{
            // orient: 'vertical',//设置其为垂直显示
            x:'0',//指的是布图的起始位置，而不是图片的起始位置。
            y:'top',
            data:[]
        },
        dataZoom:{
            show:true,
            // start:1,
            // end:50
        },
        grid: {
            y:80
        },
        calculable : true,
        xAxis:  {
            type: 'category',
            boundaryGap: false,
            data:[],
            axisLabel: {
                show: true,
                rotate: 0,
                margion: 8,
                interval:'auto'
            }
        },
        yAxis: {
            type: 'value',
            axisLabel: {
                formatter: '{value}'
            }
        },
        series: [
        ]
    }
}

function timeHandler(givenTime,inputTime){
    var time = [];
    var endTime = givenTime;
    var startTime = Math.floor((endTime-2*3600000)/1000); //默认为2小时前
    if (inputTime == '2') {
        startTime = Math.floor((endTime-6*3600000)/1000);
    } else if (inputTime == '3') {
        startTime = Math.floor((endTime-12*3600000)/1000);
    } else if (inputTime == '4') {
        startTime = Math.floor((endTime-24*3600000)/1000);
    } else if (inputTime == '5') {
        startTime = Math.floor((endTime-2*24*3600000)/1000);
    } else if (inputTime == '6') {
        startTime = Math.floor((endTime-7*24*3600000)/1000);
    }
    time.push(startTime);
    time.push(Math.floor(endTime/1000));
    return time;
}

function createReadMonitor(givenTime,qname,biz) {
    Qname = qname;
    Biz = biz;
    var time = timeHandler(givenTime,$('#time').val());
    var startTime = time[0];
    var endTime = time[1];
    //var interval = intervalHandler($('#interval').val());
    myChart = echarts.init(document.getElementById("monitorDetailInfo2"));
    option = null;
    option = createOption();
    option.title = {'text':'接收的消息数量(数量)',x: 0,y:50,'textStyle': {'fontSize': '15','fontWeight': 'bolder','color': '#333'}};
    $.ajax({
        type:"get",
        url:"/monitor",
        dataType:"json",
        data:"type=receive&queue="+qname+"&group="+biz+"&start="+startTime+"&end="+endTime,
        success: function (data) {
            console.info(data);
            legendData.push(qname+"."+biz);
            var temp = {};
            temp.name = qname+"."+biz;
            temp.type = 'line';
            temp.data = data.data;
            temp.smooth = true;
            readResult.push(temp);
            var xAxisValue = [];
            xAxisValue = data.time;
            for(var i=0;i<xAxisValue.length;i++) {
                xAxisValue[i] = new Date(xAxisValue[i]*1000).format("hh:mm");
            }
            myChart.hideLoading();
            myChart.setOption({
                legend:{
                    data:legendData
                },
                xAxis: {
                    data: xAxisValue
                },
                series: readResult
            });
        }
    });
    if (option && typeof option === "object") {
        myChart.clear();
        myChart.setOption(option, true);
    }
}

function removeReadMonitor(givenTime,qname,biz) {
    var time = timeHandler(givenTime,$('#time').val());
    var startTime = time[0];
    var endTime = time[1];
    option = null;
    option = createOption();
    option.title = {'text':'接收的消息数量(数量)',x:0,y:50,'textStyle': {'fontSize': '15','fontWeight': 'bolder','color': '#333'}}
    $.ajax({
        type:"get",
        url:"/monitor",
        dataType:"json",
        data:"type=receive&queue="+qname+"&group="+biz+"&start="+startTime+"&end="+endTime,
        success: function (data) {
            var xAxisValue = [];
            xAxisValue = data.time;
            for(var i=0;i<xAxisValue.length;i++) {
                xAxisValue[i] = new Date(xAxisValue[i]*1000).format("hh:mm");
            }
            if ($.inArray(qname+"."+biz,legendData) > -1) {
                legendData.splice($.inArray(qname+"."+biz,legendData),1);
                for(var i=0;i<readResult.length;i++) {
                    if (readResult[i].name == (qname+"."+biz)) {
                        readResult.splice(i,1);
                    }
                }
            }
            myChart.hideLoading();
            myChart.setOption({
                legend:{
                    data:legendData
                },
                xAxis: {
                    data: xAxisValue
                },
                series: readResult
            });
        }
    });
    if (option && typeof option === "object") {
        myChart.clear();
        myChart.setOption(option, true);
    }

}
function createWriteMonitor(givenTime,qname,biz) {
    var time = timeHandler(givenTime,$('#time').val());
    var startTime = time[0];
    var endTime = time[1];
    //var interval = intervalHandler($('#interval').val());
    myChart2 = echarts.init(document.getElementById("monitorDetailInfo1"));
    option2 = null;
    option2 = createOption();
    option2.title = {'text':'发送的消息数量(数量)',x:0,y:50,'textStyle': {'fontSize': '15','fontWeight': 'bolder','color': '#333'}}
    $.ajax({
        type:"get",
        url:"/monitor",
        dataType:"json",
        data:"type=send&queue="+qname+"&group="+biz+"&start="+startTime+"&end="+endTime,
        success: function (data) {
            legendWriteData.push(qname+"."+biz);
            var temp = {};
            temp.name = qname+"."+biz;
            temp.type = 'line';
            temp.data = data.data;
            temp.smooth = true;
            writeResult.push(temp);
            var xAxisValue = [];
            xAxisValue = data.time;
            for(var i=0;i<xAxisValue.length;i++) {
                xAxisValue[i] = new Date(xAxisValue[i]*1000).format("hh:mm");
            }
            myChart2.hideLoading();
            myChart2.setOption({
                legend:{
                    data:legendWriteData
                },
                xAxis: {
                    data: xAxisValue
                },
                series: writeResult
            });
        }
    });

    if (option2 && typeof option2 === "object") {
        myChart2.clear();
        myChart2.setOption(option2, true);
    }

}
function removeWriteMonitor(givenTime,qname,biz) {
    var time = timeHandler(givenTime,$('#time').val());
    var startTime = time[0];
    var endTime = time[1];
    option = null;
    option = createOption();
    option.title = {'text':'发送的消息数量(数量)',x:0,y:50,'textStyle': {'fontSize': '15','fontWeight': 'bolder','color': '#333'}}
    $.ajax({
        type:"get",
        url:"/monitor",
        dataType:"json",
        data:"type=send&queue="+qname+"&group="+biz+"&start="+startTime+"&end="+endTime,
        success: function (data) {
            var xAxisValue = [];
            xAxisValue = data.time;
            for(var i=0;i<xAxisValue.length;i++) {
                xAxisValue[i] = new Date(xAxisValue[i]*1000).format("hh:mm");
            }
            if ($.inArray(qname+"."+biz,legendWriteData) > -1) {
                legendWriteData.splice($.inArray(qname+"."+biz,legendWriteData),1);
                for(var i=0;i<writeResult.length;i++) {
                    if (writeResult[i].name == (qname+"."+biz)) {//删除名字为qname+"."+biz的元素
                        writeResult.splice(i,1);
                    }
                }
            }
            myChart2.hideLoading();
            myChart2.setOption({
                legend:{
                    data:legendData
                },
                xAxis: {
                    data: xAxisValue
                },
                series: writeResult
            });
        }
    });
    if (option && typeof option === "object") {
        myChart2.clear();
        myChart2.setOption(option, true);
    }

}
$('#time').on('changed.bs.select',function(){
    var time = new Date();
    removeReadMonitor(time,Qname,Biz);
    createReadMonitor(time,Qname,Biz);
    removeWriteMonitor(time,Qname,Biz);
    createWriteMonitor(time,Qname,Biz);
});

function refreshMonitor() {
    var time = new Date();
    removeReadMonitor(time,Qname,Biz);
    createReadMonitor(time,Qname,Biz);
    removeWriteMonitor(time,Qname,Biz);
    createWriteMonitor(time,Qname,Biz);

}

function destoryMonitor() {
    zoomDiv.style.display="none";
    $('#body').css('height',document.body.scrollHeight);
    if (myChart && myChart.dispose) {
        legendData = [];
        readResult = [];
        myChart.dispose();
    }
    if (myChart2 && myChart2.dispose) {
        legendWriteData = [];
        writeResult = [];
        myChart2.dispose();
    }
}

/*	msgOperation部分  */
function writeMessage(queueName,bizName){
    var msgText = $('#messageText').val();
    $.ajax({
        url:"/msg",
        type: "POST",
        dataType:'json',
        data: "action=send&queue="+queueName+"&group="+bizName+"&msg="+msgText,
        success:function(msg) {
            if (msg.result == true) {
                toastr.success("消息写成功!");
            }else{
                toastr.error("消息写失败!");
            }
        },
        error: function () {
            toastr.error("消息写失败!");
        }
    });
    $('#producerMessage').modal('hide');
    $('#messageText').val("");
}

function getMessage(queueName,bizName) {
    $.ajax({
        url:"/msg",
        type: "get",
        dataType:'json',
        data: "action=receive&queue="+queueName+"&group="+bizName,
        success:function(result) {
            var message = result.msg;
            message = message.replace(/\s+/g,"&nbsp;");
            if (message !=null) {
                $('#getMessageResult').html("<label class=\"col-sm-3 control-label\">消息内容为:</label>" +
                "<div class=\"col-sm-6\">" +
                "<input id=\"receive_text\" type=\"text\" class=\"form-control\" disabled value="+message+" >" +
                "</div>" );
            }
        },
        error: function () {
            toastr.error("读取消息失败!");
            /*  $("#getMessageModal").style.display="none";*/
        }
    });
}


// 子表格相关操作，通过在标签中添加onclick事件获得对应的队列名和业务名
function sendMessage(o) {
    var temp = $(o).attr("id");//获取当前元素的id号，该id号包含队列名和业务名
    var queueName = temp.split("__")[0];//双下划线
    var bizName = temp.split("__")[1];
    $('#'+temp).attr("data-toggle","modal");
    $('#'+temp).attr("data-target","#producerMessage");
    $('#producer').unbind('click');
    $('#producer').bind('click',function(){
        writeMessage(queueName,bizName);
    });
}

function receiveMessage(o) {
    var temp = $(o).attr("id");//获取当前元素的id号，该id号包含队列名和业务名
    var queueName = temp.split("__")[0];
    var bizName = temp.split("__")[1];
    $('#'+temp).attr("data-toggle","modal");
    $('#'+temp).attr("data-target","#getMessageModal");
    getMessage(queueName,bizName);
    $('#receive_text').val("");

}

/*	queueOperation部分 */
function createQueue(){
    var queueName = $('#queueName').val();
	var queueReplications = $('#replications').val();
	var queuePartitions = $('#partitions').val();
    if(queueName == "") {
        alert("queueName is necessory!");
        return;
    }

	$.ajax({
        url:"/queue",
        type: "POST",
        dataType:'json',
        data: "action=create&queue="+queueName+"&replications="+queueReplications+"&partitions="+queuePartitions,
        success:function(msg) {
            if (msg.result == true) {
                toastr.success("添加成功!");
                $table.bootstrapTable('refresh');
                $queueAction.prop('disabled',true);
                clearZoomDiv();
            }else{
                toastr.error("添加失败!");
            }
        },
        error: function () {
            toastr.error("添加失败!");
        }
    });

    $('#createQueueInfo').modal('hide');
    $('#queueName').val("");
    $('#replications').val("2");
    $('#partitions').val("16");
}

function modifyQueue() {
	var queueName = getNameSelections();
    var queueReplications = $('#modify_replications').val();
	var queuePartitions = $('#modify_partitions').val();

    for(var i=0;i<queueName.length;i++) {
       $.ajax({
        url:"/queue",
        type: "POST",
        dataType:'json',
        data: "action=update&queue="+queueName[i]+"&replications="+queueReplications+"&partitions="+queuePartitions,
        success:function(msg) {
            if (msg.result == true) {
                toastr.success("更新成功!");
                $table.bootstrapTable('refresh');
                $queueAction.prop('disabled',true);
                clearZoomDiv();
            }else{
                toastr.error("更新失败!");
            }
        },
        error: function () {
            toastr.error("更新失败!");
        }
    });
    }

    $('#modifyQueueInfo').modal('hide');
    $('#modify_replications').val("");
    $('#modify_partitions').val("");
}

function deleteQueue() {
    $('#delQueueModal').modal("show");
    $('#realDeleteQueue').unbind('click');
    $('#realDeleteQueue').bind('click',function(){
        var queueName = getNameSelections();
        for(var i=0;i<queueName.length;i++) {
            $.ajax({
                url:"/queue",
                type: "POST",
                dataType:'json',
                data: "action=remove&queue="+queueName[i],
                success:function(msg) {
                    console.info(msg);
                    if (msg.result == true) {
                        toastr.success("删除成功!");
                        $table.bootstrapTable('remove', {
                            field: 'queue',
                            values: queueName
                        });
                        clearZoomDiv();
                    }else{
                        toastr.error("删除失败!");
                    }
                },
                error: function (XMLHttpRequest, textStatus, errorThrown) {
                    toastr.error("删除失败!");
                }
            });
        }
    });
    $('#queueAction').attr('disabled','disabled');

}

function lookupQueue(queueName,biz) {
    $.ajax({
        url:"/queue",
        type:"get",
        dataType:'json',
        data:"action=lookup&queue="+queueName+"&group="+biz,  //改成按单一业务方返回；
        success:function(result) {
            createTable(result);
        }
    })
}

function getNameSelections() {
    return $.map($table.bootstrapTable('getSelections'), function (row) {
        return row.queue
    });
}

/*	table部分	*/
var chosenBizMap = new Map();
var $table = $('#table'),
$queueAction = $('#queueAction')

function parentTable() {
    $table.bootstrapTable({
        url: '/queue',
        method: 'get',
        toolbar: '#toolbar',
        cache: false,
        sortable: true,
        queryParams: 'action=lookup',
        pagination: true,
        sidePagination: "client",   //分页方式
        pagination:true,
        pageNumber:1,      //初始化加载第一页，默认第一页
        pageSize: 10,      //每页的记录行数（*）
        pageList: [10, 25, 50, 100],
        search: true,      //是否显示表格搜索
        showColumns: true,     //是否显示所有的列
        showRefresh: true,     //是否显示刷新按钮
        minimumCountColumns: 2,    //最少允许的列数
        clickToSelect: true,
        uniqueId: "queue",      //每一行的唯一标识，一般为主键列
        cardView: false,     //是否显示详细视图
        detailView: true,
        classes:'table-no-bordered',
        columns:[
        {
            field: 'state',
            checkbox: true,
            align: 'center'
        },{
            field: 'queue',
            title: '队列名称',
            align: 'center'
        },{
            field:'length',
            title:'写入量',
            align:'center'

        },{
            field: 'ctime',
            title: '创建时间',
            align: 'center',
            formatter:createTimeFormatter
        }, {
            field: 'groups',
            title: '业务方数',
            align: 'center',
            formatter: operateFormatter
        }
        ]

    });
}

function initTable() {
    parentTable();
    setTimeout(function () {
        $table.bootstrapTable('resetView');
    }, 200);
    // 只要有选择行，就让队列操作按钮可用
    $table.on('check.bs.table check-all.bs.table',function (e,row){
        $queueAction.prop('disabled',false);
    });

    // 跟子表很耦合，这样后期操作会比较难办
    $table.on('uncheck.bs.table uncheck-all.bs.table',function (e,row){
        var size = $table.bootstrapTable('getSelections').length;
        $queueAction.prop('disabled',!size);
        //当父表所有行都不选择时，让弹框消失
        if (chosenBizMap.size() == 0 || size == 0) {
            zoomDiv.style.display="none";
            $('#body').css('height',document.body.scrollHeight);
            $table.bootstrapTable('collapseAllRows');
             clearDisplayInfo();
             chosenBizMap.clear();
        }
        //这样做的原因是：如果嵌套子表，当选中或者取消选择子表中某一行时，其实就是选中或者取消选中父表中的某一行（子表是父表的某一行，父表中没有Biz列属性）。
        if (row.group == undefined ) {
            $('#bizTable_'+row.queue).bootstrapTable('uncheckAll');
            var tableData = $('#bizTable_'+row.queue).bootstrapTable('getData');
            var now = new Date().getTime();
            for (var i=0;i<tableData.length;i++) {
                removeReadMonitor(now,row.queue,tableData[i].group);
                removeWriteMonitor(now,row.queue,tableData[i].group);
            }
        }
    });

    $table.on('search.bs.table',function (){
        $queueAction.prop('disabled',true);
        clearDisplayInfo();
        chosenBizMap.clear();
        destoryMonitor();
    });

    $("button[name='refresh']").on('click',function(){
        $queueAction.prop('disabled',true);
        clearZoomDiv();
    });

    $table.on('expand-row.bs.table', function (e, index, row, $detail) {
        $.ajax({
            url:"/queue",
            type:"get",
            dataType:"json",
            data:"action=lookup&queue="+row.queue,
            success:function(msg) {
                if (msg != null) {
                    var bizs = msg[0].groups;
                    var data = [];
                    if (bizs != null) {
                        for (var i=0;i<bizs.length;i++) {
                            var biz = bizs[i];
                            biz.queue = row.queue;
                        }
                        data = bizs;
                        InitSubTable(index, row, $detail,data);
                    }
                }
            },
            error:function(res) {
                $detail.html("");
                console.info(res);
            }
        });
    });

    $(window).resize(function () {
        $table.bootstrapTable('resetView', {
            height: getHeight()
        });
    });
}

//获取所有选中的信息
function getIdSelections() {
    return $.map($table.bootstrapTable('getSelections'), function (row) {
        return row.queue
    });
}

function getHeight() {
    return $('#table').height('auto');
}

$(function () {
    var scripts = [location.search.substring(1) || 'src/dep/js/bootstrap-table.js'],
    eachSeries = function (arr, iterator, callback) {
        callback = callback || function () {};
        if (!arr.length) {
            return callback();
        }
        var completed = 0;
        var iterate = function () {
            iterator(arr[completed], function (err) {
                if (err) {
                    callback(err);
                    callback = function () {};
                }else {
                    completed += 1;
                    if (completed >= arr.length) {
                        callback(null);
                    }else {
                        iterate();
                    }
                }
            });
        };
        iterate();
    };
    eachSeries(scripts, getScript, initTable);
});

function getScript(url, callback) {
    var head = document.getElementsByTagName('head')[0];
    var script = document.createElement('script');
    script.src = url;
    var done = false;
    script.onload = script.onreadystatechange = function() {
        if (!done && (!this.readyState ||
            this.readyState == 'loaded' || this.readyState == 'complete')) {
            done = true;
        if (callback)
            callback();
        script.onload = script.onreadystatechange = null;
        }
    };
    head.appendChild(script);
    return undefined;
}

function operateFormatter(value, row, index) {
    var result = 0;
    var resultTemp = value;
    if (resultTemp != null) {
        var size = resultTemp.length;
        result = size;
    }
    return [result].join('');
}

//以双下划线作为标识
function writeFormatter (value,row,index) {
    var result = "";
    var resultTemp = value ;
    if (value) {
        result = "<input type=\"button\" id=\""+row.queue+"__"+row.group+"__createMessage\" class=\"btn btn-info \" onclick=\"sendMessage(this)\" value=\"发送\" / >"
    } else {
        result = "<input type=\"button\" id=\""+row.queue+"__"+row.group+"__createMessage\" class=\"btn btn-info \"  disabled value=\"发送\" / >"
    }
    return [result].join('');
}

//id以双下划线分隔，队列名中可能会出现下划线等字符
function readFormatter (value,row,index) {
    var result = "" ;
    var resultTemp = value ;
    if (value) {
        result = "<input type=\"button\" id=\""+row.queue+"__"+row.group+"__getMessage\" class=\"btn btn-info \" onclick=\"receiveMessage(this)\" value=\"接收\" / >"
    } else {
        result = "<input type=\"button\" id=\""+row.queue+"__"+row.group+"__getMessage\" class=\"btn btn-info \" disabled  value=\"接收\" / >"
    }
    return [result].join('');
}

function writeAmountFormatter(value,row,index) {
    var result = 0;
    return [result].join('');

}

function readAmountFormatter(value,row,index) {
    var result = 0;
    return [result].join('');

}

function urlFormatter(value,row,index) {
    var result = row.group+"."+row.queue+".intra.weibo.com";
    return [result].join('');
}

function createPeopleFormatter(value,row,index) {
    var result = "menglong";
    return [result].join('');
}

function createTimeFormatter(value,row,index){
    var result = new Date(value * 1000).format("yyyy-MM-dd hh:mm:ss");
    return [result].join('');

}

function CreateTimeFormatter(value,row,index) {
    var result = '2016-03-22 11:12:00'
    return [result].join('');
}

InitSubTable = function(index, row, $detail,data) {
     var temp = row.queue;
     var tableIndex = row.queue;
     tableIndex = tableIndex.replace(".","_"); //队列名称带"."不识别
     $detail.html("<table id=\"bizTable_" +tableIndex + "\""+" class=\"table\"></table>");
     $('#bizTable_'+tableIndex).bootstrapTable({
        checkboxHeader:false,
        uniqueId: "group",
        columns: [{
            field: 'select',
            checkbox:true,
            align:'center'

        },
        {
            field: 'queue',
            visible:false,
            align: 'center'
        },{
            field: 'group',
            title: '业务标识',
            align: 'center'
        }, {
            field: 'url',
            title: 'url',
            align: 'center',
            formatter:urlFormatter
        }, {
            field: 'sendCount',
            title: '写入量',
            align: 'center',
            formatter:writeAmountFormatter
        }, {
            field: 'receiveCount',
            title: '接收量',
            align: 'center',
            formatter:readAmountFormatter
        }, {
            field: 'createTime',
            title: '创建时间',
            align: 'center',
            formatter:CreateTimeFormatter
        }, {
            field: 'write',
            title: '发送消息',
            align: 'center',
            formatter: writeFormatter
        }, {
            field: 'read',
            title: '接收消息',
            align: 'center',
            formatter: readFormatter
        }],
        data: data
    });

   $('#bizTable_'+tableIndex).on('check.bs.table',function (e,row) {
    $('#table').bootstrapTable('check',index);
    chosenBizMap.put(row.queue+","+row.group,row.group);
    var now = new Date().getTime();//现在时间
    createReadMonitor(now,row.queue,row.group);
    createWriteMonitor(now,row.queue,row.group);
    if(chosenBizMap.size()==1) {
        lookupQueue(row.queue,row.group);
    } else if (chosenBizMap.size()>1) {
        clearDisplayInfo();
        createTableMultiChoice();
    };
    zoomDiv.style.display="block";
    $('#body').css('height',document.body.scrollHeight-$('#footer').height());
   });
   $('#bizTable_'+tableIndex).on('uncheck.bs.table',function (e,row){
     var size = $('#bizTable_'+tableIndex).bootstrapTable('getSelections').length;
     if (size == 0) {
         $('#table').bootstrapTable('uncheck', index);
     }
     var now = new Date().getTime();
     chosenBizMap.remove(row.queue+","+row.group);
     if(chosenBizMap.size()==0) {
        clearDisplayInfo();
     } else if(chosenBizMap.size()==1) {
        var chosenBizArray = chosenBizMap.entrys()[0].key.split(","); //0:队列；1:业务方
        clearDisplayInfo();
        lookupQueue(chosenBizArray[0],chosenBizArray[1]);
     }
     removeReadMonitor(now,row.queue,row.group);
     removeWriteMonitor(now,row.queue,row.group);
   })
}

Date.prototype.format = function (format) {
    var o = {
        "M+": this.getMonth() + 1,
        "d+": this.getDate(),
        "h+": this.getHours(),
        "m+": this.getMinutes(),
        "s+": this.getSeconds(),
        "q+": Math.floor((this.getMonth() + 3) / 3),
        "S": this.getMilliseconds()
    }
    if (/(y+)/.test(format)) {
        format = format.replace(RegExp.$1, (this.getFullYear() + "").substr(4 - RegExp.$1.length));
    }
    for (var k in o) {
        if (new RegExp("(" + k + ")").test(format)) {
            format = format.replace(RegExp.$1, RegExp.$1.length == 1 ? o[k] : ("00" + o[k]).substr(("" + o[k]).length));
        }
    }
    return format;
}

/**
 * Simple Map
 *
 *
 * var m = new Map();
 * m.put('key','value');
 * ...
 * var s = "";
 * m.each(function(key,value,index){
 *      s += index+":"+ key+"="+value+"/n";
 * });
 * alert(s);
 *
 * @author dewitt
 * @date 2008-05-24
 */

 Array.prototype.remove = function(s) {
    for (var i = 0; i < this.length; i++) {
        if (s == this[i])
            this.splice(i, 1);
    }
};

function Map() {
    /** 存放键的数组(遍历用到) */
    this.keys = new Array();
    /** 存放数据 */
    this.data = new Object();

    /**
     * 放入一个键值对
     * @param {String} key
     * @param {Object} value
     */
    this.put = function(key, value) {
        if(this.data[key] == null){
            this.keys.push(key);
        }
        this.data[key] = value;
    };

    /**
     * 获取某键对应的值
     * @param {String} key
     * @return {Object} value
     */
    this.get = function(key) {
        return this.data[key];
    };

    /**
     * 删除一个键值对
     * @param {String} key
     */
    this.remove = function(key) {
        this.keys.remove(key);
        this.data[key] = null;
    };

    /**
     * 遍历Map,执行处理函数
     *
     * @param {Function} 回调函数 function(key,value,index){..}
     */
    this.each = function(fn){
        if(typeof fn != 'function'){
            return;
        }
        var len = this.keys.length;
        for(var i=0;i<len;i++){
            var k = this.keys[i];
            fn(k,this.data[k],i);
        }
    };

    /**
     * 获取键值数组(类似Java的entrySet())
     * @return 键值对象{key,value}的数组
     */
    this.entrys = function() {
        var len = this.keys.length;
        var entrys = new Array(len);
        for (var i = 0; i < len; i++) {
            entrys[i] = {
                key : this.keys[i],
                // value : this.data[i]
            };
        }
        return entrys;
    };

    /**
     * 判断Map是否为空
     */
    this.isEmpty = function() {
        return this.keys.length == 0;
    };

    /**
     * 获取键值对数量
     */
    this.size = function(){
        return this.keys.length;
    };

    /**
    * 清空Map
    */
    this.clear = function(){
        var mapArray = this.entrys();
        for (var i = 0; i < mapArray.length; i++) {
            this.remove(mapArray[i].key);
        }
    };

    /**
     * 重写toString
     */
    this.toString = function(){
        var s = "{";
        for(var i=0;i<this.keys.length;i++,s+=','){
            var k = this.keys[i];
            s += k+"="+this.data[k];
        }
        s+="}";
        return s;
    };
}

/*	zoom部分
*
*
* */
$(function(){
    var src_posi_Y = 0, dest_posi_Y = 0, move_Y = 0, is_mouse_down = false, destHeight = 30;
    $("#expander")
    .mousedown(function(e){
        src_posi_Y = e.pageY;
        is_mouse_down = true;
    });
    $(document).bind("click mouseup",function(e){
        if(is_mouse_down){
          is_mouse_down = false;
        }
    })
    .mousemove(function(e){
        dest_posi_Y = e.pageY;
        move_Y = src_posi_Y - dest_posi_Y;
        src_posi_Y = dest_posi_Y;
        destHeight = $("#footer").height() + move_Y;
        if(is_mouse_down){
            $("#footer").css("height", destHeight > 30 ? destHeight : 30);
            $('#body').css('height',document.body.scrollHeight-$('#footer').height());
        }
    });
});


