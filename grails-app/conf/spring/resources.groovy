import com.mchange.v2.c3p0.ComboPooledDataSource

// Place your Spring DSL code here
beans = {

   /* dataSource(ComboPooledDataSource) { bean ->
        idleConnectionTestPeriod = 1 * 60 * 60
        testConnectionOnCheckin = true
        preferredTestQuery = "SELECT 1"
        acquireIncrement = 10
        initialPoolSize = 10
        maxPoolSize = 1200
        maxConnectionAge = 1 * 60 * 60
        maxIdleTime = 60
        bean.destroyMethod = 'close'
        user = grailsApplication.config.dataSource.username
        password = grailsApplication.config.dataSource.password
        driverClass = grailsApplication.config.dataSource.driverClassName
        jdbcUrl = grailsApplication.config.dataSource.url
    }*/
}
