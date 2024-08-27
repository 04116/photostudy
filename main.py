import duckdb
from typing import List, Dict, Tuple


class Person:
    def __init__(self, name: str, subjects: List[str], grade_level: str, style: str):
        self.name = name
        self.subjects = subjects
        self.grade_level = grade_level
        self.style = style


def create_tables(conn, teachers: List[Person], students: List[Person]):
    # Create teachers table
    conn.execute(
        """
        CREATE TABLE teachers (
            name VARCHAR,
            subjects VARCHAR,
            grade_level VARCHAR,
            style VARCHAR
        )
    """
    )

    # Insert teachers data
    for teacher in teachers:
        conn.execute(
            """
            INSERT INTO teachers VALUES (?, ?, ?, ?)
        """,
            (
                teacher.name,
                ",".join(teacher.subjects),
                teacher.grade_level,
                teacher.style,
            ),
        )

    # Create students table
    conn.execute(
        """
        CREATE TABLE students (
            name VARCHAR,
            subjects VARCHAR,
            grade_level VARCHAR,
            style VARCHAR
        )
    """
    )

    # Insert students data
    for student in students:
        conn.execute(
            """
            INSERT INTO students VALUES (?, ?, ?, ?)
        """,
            (
                student.name,
                ",".join(student.subjects),
                student.grade_level,
                student.style,
            ),
        )


def calculate_subject_overlap(x: str, y: str) -> int:
    return len(set(x.split(",")) & set(y.split(",")))


def calculate_compatibility(conn):
    conn.create_function("subject_overlap", calculate_subject_overlap, [str, str], int)

    conn.execute(
        """
        CREATE TABLE compatibility AS
        SELECT
            t.name AS teacher_name,
            s.name AS student_name,
            (CASE WHEN t.grade_level = s.grade_level THEN 1 ELSE 0 END +
             CASE WHEN t.style = s.style THEN 1 ELSE 0 END +
             subject_overlap(t.subjects, s.subjects)) AS compatibility_score
        FROM teachers t
        CROSS JOIN students s
    """
    )


def find_best_matches(conn) -> Dict[str, Tuple[str, float]]:
    result = conn.execute(
        """
        SELECT student_name, teacher_name, compatibility_score
        FROM (
            SELECT
                student_name,
                teacher_name,
                compatibility_score,
                ROW_NUMBER() OVER (PARTITION BY student_name ORDER BY compatibility_score DESC) as rn
            FROM compatibility
        )
        WHERE rn = 1
    """
    ).fetchall()

    return {student: (teacher, score) for student, teacher, score in result}


# Example usage
teachers = [
    Person("Ms. Johnson", ["Math", "Science"], "Elementary", "Visual"),
    Person("Mr. Smith", ["English", "History"], "Middle", "Auditory"),
    Person("Mrs. Davis", ["Math", "Art"], "Elementary", "Kinesthetic"),
]

students = [
    Person("Alice", ["Math", "Science"], "Elementary", "Visual"),
    Person("Bob", ["English", "History"], "Middle", "Auditory"),
    Person("Charlie", ["Math", "Art"], "Elementary", "Visual"),
]

# Create a connection to an in-memory DuckDB database
conn = duckdb.connect(":memory:")

# Create tables and insert data
create_tables(conn, teachers, students)

# Calculate compatibility scores
calculate_compatibility(conn)

# Find best matches
matches = find_best_matches(conn)

print("\n\n")
for student, (teacher, score) in matches.items():
    print(f"{student} is matched with {teacher} (Score: {score})")

# Print the compatibility matrix
print("\nCompatibility Matrix:")
print(conn.execute("SELECT * FROM compatibility").df())

# Close the connection
conn.close()
