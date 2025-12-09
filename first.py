import sqlite3

class SchoolDatabase:
    def __init__(self, db_name='school.db'):
        self.db_name = db_name
        self.conn = None
        self.cursor = None
        self.connect()

    def connect(self):
        self.conn = sqlite3.connect(self.db_name)
        self.cursor = self.conn.cursor()

    def create_tables(self):
        self.cursor.execute("""
            CREATE TABLE IF NOT EXISTS Students (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT NOT NULL,
                surname TEXT NOT NULL,
                age INTEGER,
                city TEXT
            )
        """)

        self.cursor.execute("""
            CREATE TABLE IF NOT EXISTS Courses (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT NOT NULL,
                time_start TEXT,
                time_end TEXT
            )
        """)

        self.cursor.execute("""
            CREATE TABLE IF NOT EXISTS Student_courses (
                student_id INTEGER,
                course_id INTEGER,
                FOREIGN KEY (student_id) REFERENCES Students(id) ON DELETE CASCADE,
                FOREIGN KEY (course_id) REFERENCES Courses(id) ON DELETE CASCADE,
                PRIMARY KEY (student_id, course_id)
            )
        """)
        self.conn.commit()

    def populate_data(self):
        
        self.cursor.execute("SELECT COUNT(*) FROM Courses")
        if self.cursor.fetchone()[0] == 0:
            courses_data = [
                (1, 'python', '21.07.21', '21.08.21'),
                (2, 'java', '13.07.21', '16.08.21')
            ]
            self.cursor.executemany("INSERT INTO Courses (id, name, time_start, time_end) VALUES (?, ?, ?, ?)", courses_data)

        self.cursor.execute("SELECT COUNT(*) FROM Students")
        if self.cursor.fetchone()[0] == 0:
            students_data = [
                (1, 'Max', 'Brooks', 24, 'Spb'),
                (2, 'John', 'Stones', 15, 'Spb'),
                (3, 'Andy', 'Wings', 45, 'Manchester'),
                (4, 'Kate', 'Brooks', 34, 'Spb')
            ]
            self.cursor.executemany("INSERT INTO Students (id, name, surname, age, city) VALUES (?, ?, ?, ?, ?)", students_data)

        self.cursor.execute("SELECT COUNT(*) FROM Student_courses")
        if self.cursor.fetchone()[0] == 0:
            student_courses_data = [
                (1, 1),
                (2, 1),
                (3, 1),
                (4, 2)
            ]
            self.cursor.executemany("INSERT INTO Student_courses (student_id, course_id) VALUES (?, ?)", student_courses_data)

        self.conn.commit()

    def execute_query(self, query, params=None):
        """
        Универсальная функция для выполнения произвольных SQL-запросов.
        :param query: строка с SQL-запросом
        :param params: кортеж параметров для запроса (опционально)
        :return: список кортежей с результатами запроса
        """
        if params is None:
            self.cursor.execute(query)
        else:
            self.cursor.execute(query, params)
        return self.cursor.fetchall()

    def get_students_over_30(self):
        """Получить всех студентов старше 30 лет."""
        query = "SELECT * FROM Students WHERE age > 30"
        return self.execute_query(query)

    def get_students_by_course(self, course_name):
        """Получить всех студентов, проходящих определенный курс."""
        query = """
            SELECT s.* 
            FROM Students s
            JOIN Student_courses sc ON s.id = sc.student_id
            JOIN Courses c ON sc.course_id = c.id
            WHERE c.name = ?
        """
        return self.execute_query(query, (course_name,))

    def get_students_by_course_and_city(self, course_name, city):
        """Получить всех студентов, проходящих определенный курс и из определенного города."""
        query = """
            SELECT s.* 
            FROM Students s
            JOIN Student_courses sc ON s.id = sc.student_id
            JOIN Courses c ON sc.course_id = c.id
            WHERE c.name = ? AND s.city = ?
        """
        return self.execute_query(query, (course_name, city))

    def close(self):
        """Закрытие соединения с базой данных."""
        if self.conn:
            self.conn.close()


def main():
    
    db = SchoolDatabase()

    db.create_tables()
    db.populate_data()

    print("1. Все студенты старше 30 лет:")
    for student in db.get_students_over_30():
        print(student)

    print("\n2. Все студенты, которые проходят курс по python:")
    for student in db.get_students_by_course('python'):
        print(student)

    print("\n3. Все студенты, которые проходят курс по python и из Spb:")
    for student in db.get_students_by_course_and_city('python', 'Spb'):
        print(student)

    result = db.execute_query("SELECT COUNT(*) FROM Students")
    print(f"Общее количество студентов: {result[0][0]}")

    db.close()


def test_functionality():
    """тестирование"""
    print("\n=== Тестирование ===")

    test_db = SchoolDatabase(':memory:')  

    test_db.create_tables()

    test_db.cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name IN ('Students', 'Courses', 'Student_courses')")
    tables = [row[0] for row in test_db.cursor.fetchall()]
    assert set(tables) == {'Students', 'Courses', 'Student_courses'}, "Не все таблицы были созданы"

    test_db.populate_data()

    test_db.cursor.execute("SELECT COUNT(*) FROM Students")
    assert test_db.cursor.fetchone()[0] == 4, "Неверное количество студентов"

    test_db.cursor.execute("SELECT COUNT(*) FROM Courses")
    assert test_db.cursor.fetchone()[0] == 2, "Неверное количество курсов"

    test_db.cursor.execute("SELECT COUNT(*) FROM Student_courses")
    assert test_db.cursor.fetchone()[0] == 4, "Неверное количество связей студент-курс"

    students_over_30 = test_db.get_students_over_30()
    assert len(students_over_30) == 2, "Неверное количество студентов старше 30"

    python_students = test_db.get_students_by_course('python')
    assert len(python_students) == 3, "Неверное количество студентов на курсе python"

    spb_python_students = test_db.get_students_by_course_and_city('python', 'Spb')
    assert len(spb_python_students) == 2, "Неверное количество студентов на курсе python из Spb"

    count = test_db.execute_query("SELECT COUNT(*) FROM Students WHERE city = ?", ('Spb',))
    assert count[0][0] == 3, "Неверный результат универсального запроса"

    print("Все тесты пройдены успешно!")

    test_db.close()


if __name__ == "__main__":
    main()

    test_functionality()