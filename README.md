# Generate Enrollment

## Description
Reads in a csv file that contains student records, and a parquet file that contains teacher and class records, and outputs the student, and the class they are enrolled in to a json file 

## Requirements 
- Python 3.8
- Environment that can install requirements.txt (pip, conda)

## Running Python script directly
Inside activated python environment you can run either:
- `python generate_enrollment.py`
- `python generate_enrollment.py --student_file student.csv --teacher_file teacher.parquet`

If no arguments are given the default paths are students.csv and teachers.parquet

## Output

Excpected output appears under the ./output directory

```
{
    "id":1,
    "cid":"08-2046381",
    "Student Name":"Dniren Dewbury",
    "Teacher Name":"Jessa Gibbs"
}
```
