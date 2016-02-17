package com.migrate

import grails.converters.JSON
import migration.IndexFailureInfo
import migration.Progress
import migration.ProgressIndex

class MigrateController {

    def index() {
        Long id = session.getAttribute("id");
        try {
            if (!id) {
                List progList = Progress.listOrderById();
                int size = progList.size();
                id = size > 0 ? progList[size - 1].id : 0
                session.setAttribute("id", id);
            }
        } catch (Exception e) {
            log.info e.printStackTrace()
        }
        render(view: "index", model: [id: id])
    }

    def startAgain() {
        try {
            Long id = 0
            Progress progress = new Progress()
            progress.save(flush: true, failOnError: true).refresh()
            id = progress.id
            session.setAttribute("id", id);
            MigrateJob.triggerNow([id: id])
        } catch (Exception e) {
            log.info e.printStackTrace()
        }
        redirect(action: "index")
    }

    def pause() {
        try {
            if (!MigrateJob.isInterrupted) {
                log.info "-----------------Interrupting job---------------------------"
                MigrateJob.isInterrupted = true
            }
        } catch (Exception e) {
            log.info e.printStackTrace()
        }
        render ""
    }

    def resume() {
        try {
            if (!MigrateJob.isRunning && !MigrateJob.isInterrupted) {
                Long id = session.getAttribute("id")
                MigrateJob.triggerNow([id: id])
            }
        } catch (Exception e) {
            log.info e.printStackTrace()
        }
        render ""
    }

    def loadProgressBar(Long id) {
        Progress progress = Progress.findById(id)
        def progressIndexes = progress?.progressIndexes?.size() > 0 ? progress.progressIndexes.sort({ it.id }) : null

        render(progressIndexes as JSON)
    }

    def readLogFile(Long lineNumber) {
        Map output = [:]
        File logFile = new File("${System.properties['user.home']}/MigrationLogs/MigrationTemp.log")
        LogReader logReader = new LogReader(logFile)
        try {
            logReader.seek(lineNumber);
            StringBuilder builder = new StringBuilder()
            while (logReader.hasNext()) {
                builder.append(logReader.readLine())
            }
            output = [
                    lineNumber: logReader.getLastLineRead(),
                    log       : builder.toString()
            ]
        } catch (Exception e) {

            output = [
                    lineNumber: logReader.getLastLineRead(),
                    log       : ""
            ]
        }
        render output as JSON
    }

    def failureCases(Long progressIndexId,Long offset, Long limit) {
        offset = offset ?:0
        limit = limit?:1000
        ProgressIndex progressIndex = ProgressIndex.findById(progressIndexId)
        List indexFailureInfos = IndexFailureInfo.findAllByProgressIndex(progressIndex, [max: limit, sort: "entityId", order: "asc", offset: offset])
        render(indexFailureInfos as JSON)
    }
}
