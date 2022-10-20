from pbs.cli import make_cli
from pbs.etl.preparation import prepare_data


@make_cli
def main(input_dir: str, output_path: str):
    prepare_data(input_dir, output_path)


if __name__ == "__main__":
    main()
