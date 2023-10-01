"""Queries to DB using sqlaclhemy"""

from pprint import pprint
from sqlalchemy import func, desc, select, and_

from src.models import Teacher, Student, Discipline, Grade, Group
from src.db import session



def select_1():
    r = session.query(Student.fullname, func.round(func.avg(Grade.grade), 1).label('average_grade'))\
        .select_from(Grade).join(Student).group_by(Student.id).order_by(desc('average_grade')).limit(5).all()
    return r


def select_2(discipline_id: int):
    r = session.query(Discipline.name,
                      Student.fullname,
                      func.round(func.avg(Grade.grade), 1).label('average_grade')
                      ) \
        .select_from(Grade) \
        .join(Student) \
        .join(Discipline) \
        .filter(Discipline.id == discipline_id) \
        .group_by(Student.id, Discipline.name) \
        .order_by(desc('average_grade')) \
        .limit(1).all()
    return r

def select_3(discipline_id: int):
    r = session.query(Discipline.name, Group.name, func.round(func.avg(Grade.grade), 1).label('average_grade'))\
        .select_from(Grade)\
        .join(Student)\
        .join(Discipline)\
        .join(Group)\
        .filter(Discipline.id == discipline_id)\
        .group_by(Group.name, Discipline.name)\
        .order_by(desc("average_grade"))\
        .all()
    return r

'''SELECT d.name, gr.name, ROUND(AVG(g.grade), 1 ) as average_grade
FROM grades g
JOIN students s ON s.id = g.student_id 
JOIN disciplines d  ON d.id = g.discipline_id 
JOIN [groups] gr ON gr.id = s.group_id 
WHERE d.id = 6
GROUP BY gr.name, d.name 
ORDER BY average_grade DESC;'''

def select_4():
    r = session.query(func.round(func.avg(Grade.grade), 1).label("All_groups_avg_grade")).select_from(Grade).all()
    return r
'''
SELECT ROUND(AVG(g.grade), 1) as average_grade
FROM grades g;
'''

def select_5(teacher_id: int):
    r = session.query(Teacher.fullname, Discipline.name).select_from(Discipline).join(Teacher).filter(Teacher.id == teacher_id).all()
    return r

'''
SELECT t.fullname, d.name
FROM disciplines d
JOIN teachers t ON t.id = d.teacher_id 
WHERE t.id = 4;
'''

def select_6(group_id: int):
    r = session.query(Student.fullname, Group.name).select_from(Student).join(Group).filter(Group.id == group_id).all()
    return r

'''
SELECT s.fullname, g.name 
FROM students s 
JOIN groups g ON g.id = s.group_id
WHERE g.id = 2;
'''

def select_7(group_id: int, discipline_id: int):
    r = session.query(Student.fullname, Group.name, Discipline.name, Grade.grade)\
    .select_from(Grade).join(Discipline).join(Student).join(Group).where(and_(Group.id == group_id, Discipline.id == discipline_id)).all()
    return r

'''
SELECT g.name, d.name, gr.grade
FROM students s
JOIN groups g ON g.id = s.group_id 
JOIN grades gr ON gr.student_id = s.id
JOIN disciplines d ON d.id = gr.discipline_id
WHERE g.id = 2 AND d.id =2;
'''
def select_8(teacher_id: int):
    r = session.query(Teacher.fullname, Discipline.name, func.round(func.avg(Grade.grade), 1).label("average_grade"))\
        .select_from(Grade).join(Discipline).join(Teacher).filter(Teacher.id == teacher_id).group_by(Teacher.fullname, Discipline.name).order_by(desc("average_grade")).all()
    return r

'''
SELECT t.fullname, d.name, ROUND(AVG(g.grade), 2) as average_grade
FROM grades g
JOIN disciplines d ON g.discipline_id = d.id
JOIN teachers t ON t.id = d.teacher_id
WHERE t.id = 1
GROUP BY t.fullname, d.name
ORDER BY average_grade DESC;
'''

def select_9(student_id: int):
    r = session.query(Student.fullname, Discipline.name)\
        .select_from(Grade).join(Discipline).join(Student).filter(Student.id == student_id).group_by(Student.fullname, Discipline.name).all()
    return r

'''
SELECT s.fullname, d.name
FROM grades g 
JOIN students s ON s.id = g.student_id
JOIN disciplines d ON d.id = g.discipline_id
WHERE s.id = 2
GROUP BY s.fullname, d.name;
'''

def select_10(student_id: int, teacher_id: int):
    r = session.query(Teacher.fullname, Discipline.name, Student.fullname)\
        .select_from(Grade).join(Student).join(Discipline).join(Teacher).filter(and_(Student.id == student_id, Teacher.id == teacher_id)).group_by(Teacher.fullname, Discipline.name, Student.fullname).all()
    return r


'''
SELECT t.fullname, d.name, s.fullname
FROM grades gr
JOIN disciplines d  ON d.id = gr.discipline_id 
JOIN students s ON s.id = gr.student_id
JOIN teachers t  ON t.id = d.teacher_id 
WHERE t.id = 2 AND s.id = 45
GROUP BY d.name;
'''

def select_11(student_id: int, teacher_id: int):
    r = session.query(Teacher.fullname, func.round(func.avg(Grade.grade), 1).label("average_grade"), Student.fullname)\
        .select_from(Grade).join(Student).join(Discipline).join(Teacher).filter(and_(Student.id == student_id, Teacher.id == teacher_id)).group_by(Teacher.fullname, Student.fullname).all()
    return r

'''
SELECT t.fullname, ROUND(AVG(g.grade), 2) as average_grade, s.fullname
FROM grades g
JOIN disciplines d  ON d.id = g.discipline_id 
JOIN students s ON s.id = g.student_id
JOIN teachers t  ON t.id = d.teacher_id 
WHERE t.id = 1 AND s.id = 22
GROUP BY t.fullname, s.fullname;
'''


def select_12(discipline_id, group_id):
    subquery = (select(Grade.date_of).join(Student).join(Group).where(
        and_(Grade.discipline_id == discipline_id, Group.id == group_id)
    ).order_by(desc(Grade.date_of)).limit(1).scalar_subquery())

    r = session.query(Discipline.name,
                      Student.fullname,
                      Group.name,
                      Grade.date_of,
                      Grade.grade
                      ) \
        .select_from(Grade) \
        .join(Student) \
        .join(Discipline) \
        .join(Group)\
        .filter(and_(Discipline.id == discipline_id, Group.id == group_id, Grade.date_of == subquery)) \
        .order_by(desc(Grade.date_of)) \
        .all()
    return r


if __name__ == '__main__':
    # pprint(select_1())
    # pprint(select_2(2))
    # pprint(select_3(1))
    # pprint(select_4())
    # pprint(select_5(2))
    # pprint(select_6(2))
    # pprint(select_7(3, 3))
    # pprint(select_8(5))
    # pprint(select_9(4))
    # pprint(select_10(12, 3))
    # pprint(select_11(11, 3))
    pprint(select_12(1, 1))