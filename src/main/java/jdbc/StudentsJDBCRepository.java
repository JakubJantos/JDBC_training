package jdbc;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import javax.sql.DataSource;

import jdbc.exception.RepositoryException;
import jdbc.model.Student;

class StudentsJDBCRepository {

    private final DataSource dataSource;

    StudentsJDBCRepository(DataSource datasource) {
        this.dataSource = datasource;
    }

    List<Student> students = new ArrayList<>();

    public List<Student> findAllStudents() {
        try (
                Connection c = dataSource.getConnection();
                Statement s = c.createStatement();
                ResultSet rs = s.executeQuery("SELECT * FROM students")
        ) {
            while (rs.next()) {
                students.add(new Student(
                        rs.getLong("id"),
                        rs.getString("first_name"),
                        rs.getString("last_name"),
                        rs.getDate("birthdate").toLocalDate()
                ));
            }

            return students;

        } catch (SQLException e) {
            throw new RepositoryException(e);
        }
    }

    public int countStudents() {
        try (
                Connection c = dataSource.getConnection();
                Statement s = c.createStatement();
                ResultSet rs = s.executeQuery("SELECT count(id) AS counter FROM students")
        ) {
            int licz = 0;
            if (rs.next()) {
                licz = rs.getInt("counter");
            }
            return licz;

        } catch (SQLException e) {
            throw new RepositoryException(e);
        }
    }


    public Optional<Student> findStudentById(long id) {


        try (
                Connection c = dataSource.getConnection();
                PreparedStatement ps = c.prepareStatement("SELECT * FROM students WHERE id = ?");

        ) {
            ps.setLong(1, id);
            try (ResultSet rs = ps.executeQuery()) {


                if (rs.next()) {
                    return Optional.of(new Student(
                            rs.getLong("id"),
                            rs.getString("first_name"),
                            rs.getString("last_name"),
                            rs.getDate("birthdate").toLocalDate()
                    ));
                } else {
                    return Optional.empty();
                }
            }

        } catch (SQLException e) {
            throw new RepositoryException(e);
        }
    }

    public Student createStudent(Student student) {
        try (
                Connection c = dataSource.getConnection();
                PreparedStatement ps = c.prepareStatement("INSERT INTO students(first_name, last_name, birthdate) VALUES (?,?,?)", Statement.RETURN_GENERATED_KEYS);
        ) {

            ps.setString(1, student.getFirstName());
            ps.setString(2, student.getLastName());
            ps.setDate(3, Date.valueOf(student.getBirthdate()));

            ps.executeUpdate();

            ResultSet rs = ps.getGeneratedKeys();

            if (rs.next()) {
                long newID = rs.getLong(1);
                return new Student(newID, student.getFirstName(), student.getLastName(), student.getBirthdate());
            } else {
                throw new RepositoryException("Coldn't retrive.key");
            }

        } catch (SQLException e) {
            throw new RepositoryException(e);
        }
    }

    public Student updateStudent(Student student) {
        try (
                Connection c = dataSource.getConnection();
                PreparedStatement ps = c.prepareStatement("UPDATE students set first_name = ?, last_name = ?, birthdate = ?  where id = ?");
        ) {


            ps.setString(1, student.getFirstName());
            ps.setString(2, student.getLastName());
            ps.setDate(3, Date.valueOf(student.getBirthdate()));
            ps.setLong(4, student.getId());
            int noOfUpdatetRows = ps.executeUpdate();

            if (noOfUpdatetRows == 1) {
                return student;
            } else {
                throw new RepositoryException("Couldn't find student with id=10 to perform update.");
            }


        } catch (SQLException e) {
            throw new RepositoryException(e);
        }
    }

    public void deleteStudent(long id) {
        try (
                Connection c = dataSource.getConnection();
                PreparedStatement ps = c.prepareStatement("DELETE FROM school_class_students WHERE student_id = ?");
                PreparedStatement ps2 = c.prepareStatement("DELETE FROM students WHERE id = ?");
        ) {

            c.setAutoCommit(false);
            ps.setLong(1, id);
            ps2.setLong(1, id);
            int tmp = ps.executeUpdate();
            int tmp2 = ps2.executeUpdate();


            if (tmp2 == 1) {
                c.commit();
            } else {
                c.rollback();
                throw new RepositoryException("Couldn't find student with id=10 to perform delete.");
            }


        } catch (SQLException e) {
            throw new RepositoryException(e);
        }
    }


    public List<Student> findStudentsByName(String name) {
        try (
                Connection c = dataSource.getConnection();
                PreparedStatement ps = c.prepareStatement("SELECT * FROM students WHERE first_name LIKE ? OR last_name LIKE ?");
        ) {

            ps.setString(1, name + "%");
            ps.setString(2, name + "%");

            ps.executeQuery();

            try (ResultSet rs = ps.executeQuery();) {
                List<Student> listOfStudents = new ArrayList<>();

                while (rs.next()) {
                    listOfStudents.add(new Student(
                            rs.getLong("id"),
                            rs.getString("first_name"),
                            rs.getString("last_name"),
                            rs.getDate("birthdate").toLocalDate()
                    ));
                }

                return listOfStudents;
            }
        } catch (SQLException e) {
            throw new RepositoryException(e);
        }
    }


    public List<Student> findStudentsByTeacherId(long id) {
        try (
                Connection c = dataSource.getConnection();
                PreparedStatement ps = c.prepareStatement("SELECT students.id, first_name, last_name, birthdate FROM school_classes JOIN school_class_students ON school_classes.id = school_class_students.school_class_id JOIN students ON students.id = school_class_students.student_id WHERE school_classes.teacher_id = ?");

        ) {

            ps.setString(1, String.valueOf(id));

            ps.executeQuery();

            try (ResultSet rs = ps.executeQuery()) {

                List<Student> stutdentsByTeacherID = new ArrayList<>();

                while (rs.next()) {
                    stutdentsByTeacherID.add(new Student(
                            rs.getLong("id"),
                            rs.getString("first_name"),
                            rs.getString("last_name"),
                            rs.getDate("birthdate").toLocalDate()
                    ));
                }
                return stutdentsByTeacherID;
            }
        } catch (SQLException e) {
            throw new RepositoryException(e);
        }
    }

    Optional<Double> getAverageAge() {
        try (
                Connection c = dataSource.getConnection();
                Statement s = c.createStatement();
                ResultSet rs = s.executeQuery("select AVG(TIMESTAMPDIFF( YEAR , birthdate, current_date)) as AVG_AGE from students")
        ) {
            if (rs.next()){
                return Optional.of(rs.getDouble(1));
            }else {
                return Optional.empty();
            }

        } catch (SQLException e) {
            throw new RepositoryException(e);
        }
    }
}
