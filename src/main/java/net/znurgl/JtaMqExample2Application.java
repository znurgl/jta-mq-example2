package net.znurgl;

import org.h2.jdbcx.JdbcDataSource;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.jta.XADataSourceWrapper;
import org.springframework.context.annotation.Bean;
import org.springframework.core.io.ClassPathResource;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.init.DataSourceInitializer;
import org.springframework.jdbc.datasource.init.ResourceDatabasePopulator;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.web.bind.annotation.*;

import javax.sql.DataSource;
import java.util.Collection;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

@SpringBootApplication
public class JtaMqExample2Application {

    private final XADataSourceWrapper wrapper;

    public JtaMqExample2Application(XADataSourceWrapper wrapper) {
        this.wrapper = wrapper;
    }

    public static void main(String[] args) {
		SpringApplication.run(JtaMqExample2Application.class, args);
	}

	@Bean
    DataSource a() throws Exception {
        return this.wrapper.wrapDataSource( dataSource("a") );
    }

    @Bean
    DataSource b() throws Exception {
        return this.wrapper.wrapDataSource( dataSource("b") );
    }

    private JdbcDataSource dataSource(String ds) {
        JdbcDataSource jdbcDataSource = new JdbcDataSource();
        jdbcDataSource.setURL("jdbc:h2:mem:" + ds);
        jdbcDataSource.setUser("sa");
        jdbcDataSource.setPassword("");
        return jdbcDataSource;
    }

    @Bean
    DataSourceInitializer aInit(DataSource a) {
        return init(a, "a");
    }

    @Bean
    DataSourceInitializer bInit(DataSource b) {
        return init(b, "b");
    }

    private DataSourceInitializer init(DataSource ds, String a) {
        DataSourceInitializer dsi = new DataSourceInitializer();
        dsi.setDataSource(ds);
        dsi.setDatabasePopulator(new ResourceDatabasePopulator(new ClassPathResource(a + ".sql")));
        return dsi;
    }

    @RestController
    public static class JtaMqApiRestController {

        private final JdbcTemplate a;
        private final JdbcTemplate b;

        public JtaMqApiRestController(DataSource a, DataSource b) {
            this.a = new JdbcTemplate(a);
            this.b = new JdbcTemplate(b);
        }

        /**
         * curl http://localhost:8090/pets
         *
         * @return
         */
        @GetMapping("/pets")
        public Collection<String> pets() {
            return this.a.query("select * from pet", (resultSet, i) -> resultSet.getString("nickname"));
        }

        /**
         * curl http://localhost:8090/messages
         *
         * @return
         */
        @GetMapping("/messages")
        public Collection<String> messages() {
            return this.b.query("select * from message", (resultSet, i) -> resultSet.getString("message"));
        }

        /**
         * curl -d'{ "name": "Joe Black" }' -H"content-type: application/json" http://localhost:8090
         * curl -d'{ "name": "Joe Black" }' -H"content-type: application/json" http://localhost:8090?rollback=true
         *
         * @param payload
         * @param rollback
         */
        @PostMapping
        @Transactional
        public void write(@RequestBody Map<String, String> payload,
                          @RequestParam Optional<Boolean> rollback) {

            String name = payload.get("name");
            String msg = "Hello, " + name;
            this.a.update("insert into pet (id, nickname) values(?,?)", UUID.randomUUID().toString(), name);
            this.b.update("insert into message (id, message) values(?,?)", UUID.randomUUID().toString(), msg);

            if( rollback.orElse(false) ){
                throw new RuntimeException("couldn't write the message");
            }
        }
    }
}
