package jdbc;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import jdbc.model.Teacher;
import org.springframework.jdbc.core.RowMapper;

public class TeacherRowMapper implements RowMapper<Teacher> {
  @Override
  public Teacher mapRow(ResultSet resultSet, int i) throws SQLException {

    return new Teacher(
    resultSet.getLong("id"),
    resultSet.getString("first_name"),
    resultSet.getString("last_name")
    );

  }
}
