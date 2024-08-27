import duckdb
from typing import List, Dict, Tuple


class Person:
    def __init__(self, name: str, attributes: Dict[str, str]):
        self.name = name
        self.attributes = attributes


def create_tables(conn, teachers: List[Person], students: List[Person]):
    # Create entities table
    conn.execute(
        """
        CREATE TABLE entities (
            id INTEGER PRIMARY KEY,
            name VARCHAR,
            type VARCHAR
        )
    """
    )

    # Create attributes table
    conn.execute(
        """
        CREATE TABLE attributes (
            entity_id INTEGER,
            attribute VARCHAR,
            value VARCHAR,
            FOREIGN KEY (entity_id) REFERENCES entities(id)
        )
    """
    )

    # Insert teachers data
    for i, teacher in enumerate(teachers, start=1):
        conn.execute(
            "INSERT INTO entities VALUES (?, ?, ?)", (i, teacher.name, "teacher")
        )
        for attr, value in teacher.attributes.items():
            conn.execute("INSERT INTO attributes VALUES (?, ?, ?)", (i, attr, value))

    # Insert students data
    for i, student in enumerate(students, start=len(teachers) + 1):
        conn.execute(
            "INSERT INTO entities VALUES (?, ?, ?)", (i, student.name, "student")
        )
        for attr, value in student.attributes.items():
            conn.execute("INSERT INTO attributes VALUES (?, ?, ?)", (i, attr, value))


def calculate_compatibility(conn):
    conn.create_function(
        "subject_overlap",
        lambda x, y: len(set(x.split(",")) & set(y.split(","))),
        [str, str],
        int,
    )

    conn.execute(
        """
        CREATE TABLE compatibility AS
        WITH teacher_attrs AS (
            SELECT e.id, e.name,
                   MAX(CASE WHEN a.attribute = 'subjects' THEN a.value END) AS subjects,
                   MAX(CASE WHEN a.attribute = 'grade_level' THEN a.value END) AS grade_level,
                   MAX(CASE WHEN a.attribute = 'style' THEN a.value END) AS style
            FROM entities e
            JOIN attributes a ON e.id = a.entity_id
            WHERE e.type = 'teacher'
            GROUP BY e.id, e.name
        ),
        student_attrs AS (
            SELECT e.id, e.name,
                   MAX(CASE WHEN a.attribute = 'subjects' THEN a.value END) AS subjects,
                   MAX(CASE WHEN a.attribute = 'grade_level' THEN a.value END) AS grade_level,
                   MAX(CASE WHEN a.attribute = 'style' THEN a.value END) AS style
            FROM entities e
            JOIN attributes a ON e.id = a.entity_id
            WHERE e.type = 'student'
            GROUP BY e.id, e.name
        )
        SELECT
            t.name AS teacher_name,
            s.name AS student_name,
            (CASE WHEN t.grade_level = s.grade_level THEN 1 ELSE 0 END +
             CASE WHEN t.style = s.style THEN 1 ELSE 0 END +
             subject_overlap(t.subjects, s.subjects)) AS compatibility_score
        FROM teacher_attrs t
        CROSS JOIN student_attrs s
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
    Person(
        "Ms. Johnson",
        {"subjects": "Math,Science", "grade_level": "Elementary", "style": "Visual"},
    ),
    Person(
        "Mr. Smith",
        {"subjects": "English,History", "grade_level": "Middle", "style": "Auditory"},
    ),
    Person(
        "Mrs. Davis",
        {"subjects": "Math,Art", "grade_level": "Elementary", "style": "Kinesthetic"},
    ),
]

students = [
    Person(
        "Alice",
        {"subjects": "Math,Science", "grade_level": "Elementary", "style": "Visual"},
    ),
    Person(
        "Bob",
        {"subjects": "English,History", "grade_level": "Middle", "style": "Auditory"},
    ),
    Person(
        "Charlie",
        {"subjects": "Math,Art", "grade_level": "Elementary", "style": "Visual"},
    ),
]

# Create a connection to an in-memory DuckDB database
conn = duckdb.connect(":memory:")

# Create tables and insert data
create_tables(conn, teachers, students)

# Calculate compatibility scores
calculate_compatibility(conn)

# Find best matches
matches = find_best_matches(conn)

for student, (teacher, score) in matches.items():
    print(f"{student} is matched with {teacher} (Score: {score})")

# Print the compatibility matrix
print("\nCompatibility Matrix:")
print(conn.execute("SELECT * FROM compatibility").df())

# Close the connection
conn.close()
