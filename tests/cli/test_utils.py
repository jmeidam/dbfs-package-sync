from dbfsps.cli import utils


def test_create_requirements_file(mocker):
    f_check_call = mocker.patch('dbfsps.cli.utils.subprocess.check_call')
    utils.create_requirements_file()
    args = ["poetry", "export", "-f", "requirements.txt", "--output", "requirements.txt"]
    f_check_call.assert_called_once_with(args)
