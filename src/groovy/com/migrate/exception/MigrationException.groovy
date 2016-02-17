package com.migrate.exception

/**
 * Created by rubinder on 31/8/15.
 */
class MigrationException extends GroovyRuntimeException {

    MigrationException(String message) {
        super(message)
    }

    MigrationException() {
    }
}
