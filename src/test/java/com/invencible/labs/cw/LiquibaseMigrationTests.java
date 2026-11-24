package com.invencible.labs.cw;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.JdbcTemplate;

@SpringBootTest
class LiquibaseMigrationTests {

	@Autowired
	private JdbcTemplate jdbcTemplate;

	@Test
	void changelogIsAppliedToInMemoryDatabase() {
		List<String> applied = jdbcTemplate.queryForList(
				"SELECT id FROM DATABASECHANGELOG ORDER BY orderexecuted", String.class);

		assertThat(applied).contains("001-baseline");
	}

	@Test
	void databaseIsH2InMemory() {
		String url = jdbcTemplate.execute((java.sql.Connection c) -> c.getMetaData().getURL());

		assertThat(url).startsWith("jdbc:h2:mem:");
	}

}
