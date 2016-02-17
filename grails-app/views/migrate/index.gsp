<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="utf-8">
    <meta http-equiv="X-UA-Compatible" content="IE=edge">
    <meta name="viewport" content="width=device-width, initial-scale=1">
    <title>Data Migration</title>
    <asset:stylesheet href="bootstrap.min"/>
    <asset:stylesheet href="migration"/>
    <asset:stylesheet href="g.paginate.override"/>
    <asset:stylesheet href="jquery.dataTables.min"/>
</head>

<body>
<div class="container">
    <nav class="navbar navbar-inverse navbar-fixed-top">
        <div class="navbar-header">
            <div class="navbar-brand">Data Migration</div>
        </div>
    </nav>

</div>

<div class="container theme-showcase">
    <g:link action="startAgain"><input id="startAgain" type="button" class="btn btn-danger"
                                       value="Start Again"/></g:link>
    <input id="pause" type="button" class="btn btn-primary" value="Pause Migration"/>
    <input id="resume" type="button" class="btn btn-primary" value="Resume Migration"/>
    <br/><br/>


    <div class="panel panel-primary">
        <div class="panel-heading">Progress</div>

        <div class="panel-body">
            <b>Completed Entities Information:</b>

            <div class="well progressInfo" style="height: auto"></div>

            <b>Current Entity Progress:</b><br/>
            <i>Processed:</i>

            <div class="progress">
                <div class="progress-bar progress-bar-warning processed-bar" role="progressbar"
                     style="min-width: 2em; width: 0%">
                    0%
                </div>
            </div>

            <i>Migrated:</i>

            <div class="progress">
                <div class="progress-bar migrated-bar" role="progressbar" style="min-width: 2em; width: 0%">
                    0%
                </div>
            </div>
        </div>

        <div class="panel-footer"></div>
    </div>
</div>


<div class="container logs">
    <div class="panel panel-primary">
        <div class="panel-heading">Logs</div>

        <div class="panel-body">
            <div class="well">

            </div>
        </div>
    </div>
</div>

<div class="container errors">
    <div class="panel panel-primary">
        <div class="panel-heading">Errors</div>

        <div class="panel-body">
            <div class="well">

            </div>
        </div>
    </div>
</div>

<div class="container faliures">
    <div class="col-sm-3">
        <select class="form-control" id="progressIndexes">
        </select>
    </div>
    <div class="col-sm-3">
        <input class="form-control" id="offset" value="" placeholder="offset">
    </div>
    <div class="col-sm-3">
        <input class="form-control" id="limit" value="" placeholder="limit">
    </div>
    <div class="col-sm-3">
        <input id="getFailureCases" type="button" class="btn btn-primary" value="Fetch Failed Cases"/>
    </div>
    <br/>
    <br/>
    <div class="col-sm-12" class="well"  style="height: auto">
        <table id="table_data_table" class="table hover table-bordered table-striped dataTable no-footer" role="grid"
               aria-describedby="table_data_table_info">
            <thead>
            <tr role="row">
                <th class="sorting_asc" tabindex="0" aria-controls="table_data_table" rowspan="1" colspan="1">Entity Id</th>
                <th class="sorting" tabindex="0" aria-controls="table_data_table" rowspan="1" colspan="1">Failure Reason</th>
            </tr>
            </thead>
            <tbody>
            </tbody>
        </table>
    </div>
</div>




<input type="hidden" id="progressURL" value="${g.createLink(controller: "migrate", action: "loadProgressBar")}"/>
<input type="hidden" id="logURL" value="${g.createLink(controller: "migrate", action: "readLogFile")}"/>
<input type="hidden" id="logLine" value="-1"/>
<input type="hidden" id="progressId" value="${id}"/>
<input type="hidden" id="pauseURL" value="${g.createLink(controller: "migrate", action: "pause")}"/>
<input type="hidden" id="resumeURL" value="${g.createLink(controller: "migrate", action: "resume")}"/>
<input type="hidden" id="failureCaseURL" value="${g.createLink(controller: "migrate", action: "failureCases")}"/>

<g:javascript library="jquery" plugin="jquery"/>
<asset:javascript src="bootstrap.min"/>
<asset:javascript src="migration"/>
<asset:javascript src="jquery.dataTables.js"/>
</body>
</html>