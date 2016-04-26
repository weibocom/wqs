//增加按回车键触发事件
$(function(){
    $('#createQueueInfo').keydown(
        function(){
            if (event.keyCode == "13") {//keyCode=13是回车键
                $('#confirmCreate').click();
            }
        });
    $('#modifyQueueInfo').keydown(
        function(){
            if(event.keyCode == "13") {
                $('#confirmModify').click();
            }
        });
    $('#alarmSetting').keydown(
        function(){
            if(event.keyCode == "13") {
                $('#confirmSetting').click();
            }
        });
    $('#producerMessage').keydown(
        function(){
            if(event.keyCode == "13") {
                $('#producer').click();
            }
        }
        );
    $('#messageText').bind('keypress',function(event){
            if(event.keyCode == "13") {
                event.preventDefault();
                $('#producerMessage').focus();
            }
        }
        );

    $('#addBizs').keydown(
        function(){
            if(event.keyCode == "13") {
                $('#commitAddBizs').click();
            }
        });
    $('#modifyBizs').keydown(
        function(){
            if(event.keyCode == "13") {
                $('#commitModifyBizs').click();
            }
        });
    $('#delQueueModal').keydown(
        function(){
            if(event.keyCode == "13") {
                $('#realDeleteQueue').click();
            }
        });
    $('#delBizModal').keydown(
        function(){
            if(event.keyCode == "13") {
                $('#realDeleteBiz').click();
            }
        });
});
