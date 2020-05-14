"""
A script that combines student and teacher data to output class enrollment to
json.
Dask dataframes allow for larger than memory computing on a single machine
"""
import os
import argparse
import dask.dataframe as dd

def main():
    """
    Reads student and teacher day from command line arguments, combines first
    and last names, and joins the data on cid, and then writes to a json file.
    """
    arg_parser = create_arg_parser()
    parser_args = arg_parser.parse_args()

    student_df = dd.read_csv(
        parser_args.student_file,
        sep="_",
        usecols=['id', 'fname', 'lname', 'cid']
    )

    student_df['Student Name'] = student_df.fname + " " + student_df.lname
    student_df = student_df.drop(labels=['fname', 'lname'], axis=1)

    teachers_df = dd.read_parquet(
        parser_args.teacher_file,
        engine='pyarrow',
        columns=['fname', 'lname', 'cid']
    ).set_index('cid')

    teachers_df['Teacher Name'] = teachers_df.fname + " " + teachers_df.lname
    teachers_df = teachers_df.drop(labels=['fname', 'lname'], axis=1)

    final_df = dd.merge(
        student_df,
        teachers_df,
        left_on='cid',
        right_index=True
    )

    final_df.to_json('output', index=True)
    os.rename("./output/0.part", "./output/data.json")

def create_arg_parser():
    """
    Setup for argparser variables --student_file, and --teacher_file
    """
    parser = argparse.ArgumentParser(description="Reads student and teacher " \
        "records to output student and their enrolled classes to json")

    parser.add_argument(
        "--student_file",
        help="Path to student csv file",
        default="students.csv")

    parser.add_argument(
        "--teacher_file",
        help="Path to teacher parquet file",
        default="teachers.parquet")
    return parser

if __name__ == "__main__":
    main()
