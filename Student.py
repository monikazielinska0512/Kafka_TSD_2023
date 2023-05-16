class Student:
    def __init__(self, name, marks):
        self.name = name
        self.marks = marks

    def calculate_gpa(self):
        total_marks = sum(self.marks)
        gpa = total_marks / len(self.marks)
        return gpa
