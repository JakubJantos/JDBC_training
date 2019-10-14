package jdbc;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import javax.sql.DataSource;

import jdbc.model.Teacher;
import org.springframework.jdbc.core.BatchPreparedStatementSetter;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.jdbc.support.GeneratedKeyHolder;
import org.springframework.jdbc.support.KeyHolder;

public class TeachersJDBCRepository {

    private final JdbcTemplate jdbcTemplate;
    private final NamedParameterJdbcTemplate namedParameterJdbcTemplate;

    public TeachersJDBCRepository(DataSource dataSource) {
        jdbcTemplate = new JdbcTemplate(dataSource);
        namedParameterJdbcTemplate = new NamedParameterJdbcTemplate(dataSource);
    }

    public Optional<Teacher> findTeacherById(Long id) {
        Optional<Teacher> query = namedParameterJdbcTemplate.query("SELECT * FROM teachers WHERE  id = :ID",
                new MapSqlParameterSource("ID", id),
                new TeacherResultSetExtractor());

        return query;
    }

    public List<Teacher> findAllTeachers() {

        List<Teacher> teachersQuery = jdbcTemplate.query("SELECT * FROM teachers", new TeacherRowMapper());

        return teachersQuery;
    }

    public List<Teacher> findTeachersWhere(String query) {

        List<Teacher> teachersFinder = namedParameterJdbcTemplate.query("SELECT teachers.id, teachers.first_name, teachers.last_name FROM teachers\n" +
                        "JOIN school_classes ON school_classes.teacher_id = teachers.id\n" +
                        "WHERE LOWER(teachers.first_name) LIKE LOWER(:query)\n" +
                        "      OR LOWER(teachers.last_name) LIKE LOWER(:query)\n" +
                        "      OR LOWER(school_classes.name) LIKE LOWER(:query)",
                new MapSqlParameterSource("query", query + "%"),
                new TeacherRowMapper());

        return teachersFinder;

    }

    public Teacher createTeacher(Teacher teacher) {
        KeyHolder kh = new GeneratedKeyHolder();

        namedParameterJdbcTemplate.update(
                "INSERT INTO teachers(first_name, last_name) VALUES (:first_name, :last_name)",
                new MapSqlParameterSource("first_name", teacher.getFirstName())
                        .addValue("last_name", teacher.getLastName()), kh);

        return new Teacher(kh.getKey().longValue(),
                teacher.getFirstName(),
                teacher.getLastName());
    }

    public int batchCreateTeachers(List<Teacher> teachers) {

        int[]ic = jdbcTemplate.batchUpdate(
                "INSERT INTO teachers(first_name, last_name) VALUES (?,?)",
                new BatchPreparedStatementSetter() {
                    @Override
                    public void setValues(PreparedStatement preparedStatement, int i) throws SQLException {
                        preparedStatement.setString(1, teachers.get(i).getFirstName());
                        preparedStatement.setString(2, teachers.get(i).getLastName());
                    }

                    @Override
                    public int getBatchSize() {
                        return teachers.size();
                    }
                }
        );
        return Arrays.stream(ic).sum();
    }
}

