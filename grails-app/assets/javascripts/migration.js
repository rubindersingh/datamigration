var timeOutProgressBar;
var timeOutLog;
var currentIndex = "";
var allIndexes = new Object();
var mainIndexCount = 0;
(function ($) {
    $(function () {
        function reloadProgressBar() {
            $.ajax($("#progressURL").val(), {
                data: {
                    id: $("#progressId").val()
                },
                success: function (data) {
                    if (!jQuery.isEmptyObject(data)) {
                        var currentProgressIndex = null;
                        var indexCount = 0;
                        $(".progressInfo").empty();
                        $.each(data, function (key, value) {
                            if (!value.isCurrent) {

                                $(".progressInfo").append("<div class='progressIndex'>Index: <b>" + value.indexName + "</b> | Processed: <b>" + value.countProcessed + "</b> | Migrated: <b>" + value.countMigrated + "</b> | total: <b>" + value.totalDocuments + "</b> | Success: <b>" + value.countValid + "</b> | Failed: <b>" + value.countFail + "</b> | status: <b>" + value.status.name + "</b> | Total Time: <b>" + dateDifference(value.progressStart,value.progressEnd) + "</b> <div style='margin-bottom: 10px;border-top: 2px double #006dba;'></div></div>");
                            }
                            else {
                                currentProgressIndex = value;
                            }
                            allIndexes[value.id] = value.indexName;
                            indexCount++;
                        });

                        if(currentProgressIndex!=null)
                        {
                            if (currentProgressIndex.indexName != currentIndex) {
                                $(".migrated-bar").css("width", "0%").html("0%");
                                $(".processed-bar").css("width", "0%").html("0%");
                                currentIndex = currentProgressIndex.indexName;
                            }

                            var processedPercentage = 0;
                            var migratedPercentage = 0;
                            if (currentProgressIndex.totalDocuments > 0) {
                                processedPercentage = Math.round((currentProgressIndex.countProcessed / currentProgressIndex.totalDocuments) * 100);
                                migratedPercentage = Math.round((currentProgressIndex.countMigrated / currentProgressIndex.totalDocuments) * 100);
                            }
                            $(".migrated-bar").removeClass().addClass("progress-bar progress-bar-info migrated-bar");
                            $(".panel-footer").html("Index: <b>" + currentProgressIndex.indexName + "</b> | Processed: <b>" + currentProgressIndex.countProcessed + "</b> | Migrated: <b>" + currentProgressIndex.countMigrated + "</b> | total: <b>" + currentProgressIndex.totalDocuments + "</b> | Success: <b>" + currentProgressIndex.countValid + "</b> | Failed: <b>" + currentProgressIndex.countFail + "</b> | status: <b>" + currentProgressIndex.status.name + "</b>");
                            $(".migrated-bar").css("width", migratedPercentage + "%").html(migratedPercentage + "%");
                            $(".processed-bar").css("width", processedPercentage + "%").html(processedPercentage + "%");
                            if (currentProgressIndex.bulkErrorMessage.length > 0)
                                $(".errors .well").html(currentProgressIndex.bulkErrorMessage)
                        }
                        else
                        {
                            $(".migrated-bar").css("width", "0%").html("0%");
                            $(".processed-bar").css("width", "0%").html("0%");
                            $(".panel-footer").html("");
                        }


                        if(mainIndexCount != indexCount)
                        {
                            mainIndexCount = indexCount;
                            $("#progressIndexes").empty();
                            $.each(allIndexes, function (key, value) {
                                $("#progressIndexes").append("<option value='"+key+"'>"+value+"</option>");
                            });
                        }

                    }
                    timeOutProgressBar = setTimeout(reloadProgressBar, 2000);
                },
                error: function (data) {
                    timeOutProgressBar = setTimeout(reloadProgressBar, 2000);
                }
            });
        }

        function reloadLog() {
            $.ajax($("#logURL").val(), {
                data: {
                    lineNumber: $("#logLine").val()
                },
                success: function (data) {
                    $("#logLine").val(data.lineNumber);
                    if (!jQuery.isEmptyObject(data.log)) {
                        $(".logs .well").append(data.log);
                    }

                    //$(".well").prop("scrollTop", $(".well").prop("scrollHeight"));
                    timeOutLog = setTimeout(reloadLog, 10000);
                },
                error: function (data) {
                    timeOutLog = setTimeout(reloadLog, 10000);
                }
            });
        }

        setTimeout(reloadProgressBar, 1000);
        setTimeout(reloadLog, 1000);

        //$('#pause').attr('disabled',true);
        $('#pause').click(function () {
            $.ajax($("#pauseURL").val(), {
                success: function (data) {

                },
                error: function (data) {
                    console.log(data)
                }
            });
        });
        $('#resume').click(function () {
            $.ajax($("#resumeURL").val(), {
                success: function (data) {

                },
                error: function (data) {
                    console.log(data)
                }
            });
        });

        $('#getFailureCases').click(function () {
            $("#table_data_table tbody").empty()
            $.ajax($("#failureCaseURL").val(), {
                data: {
                    progressIndexId: $("#progressIndexes").val(),
                    offset : $("#offset").val(),
                    limit : $("#limit").val()
                },
                success: function (data) {
                    if (!jQuery.isEmptyObject(data)) {
                        $.each(data, function (key, value) {
                            $("#table_data_table tbody").append("<tr><td>"+value.entityId+"</td><td>"+value.failureReason+"</td></tr>");
                        });
                    }
                }
            });
        });

    });
})(jQuery);

function dateDifference(startDate,endDate)
{
    startDate = Date.parse(startDate);
    endDate = Date.parse(endDate);

    var delta = Math.abs(endDate - startDate) / 1000;

    // calculate (and subtract) whole hours
    var hours = Math.floor(delta / 3600) % 24;
    delta -= hours * 3600;

    // calculate (and subtract) whole minutes
    var minutes = Math.floor(delta / 60) % 60;
    delta -= minutes * 60;

    // what's left is seconds
    var seconds = delta % 60;

    return hours+" hours "+minutes+" minutes "+seconds+" seconds"
}