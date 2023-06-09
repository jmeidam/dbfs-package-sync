from dbfsps.syncer.file import File, sort_list_of_files


def test_sort_list_of_files():
    paths = [
        "notebook.py",
        "file1.py",
        "module/file2.py",
        "module/submodule/z.py",
        "module/b.py",
        "module/submodule/x.py",
        "module/submodule/y.py",
        "module/a_submodule/a.py",
    ]
    expected_paths = [
        "file1.py",
        "notebook.py",
        "module/b.py",
        "module/file2.py",
        "module/a_submodule/a.py",
        "module/submodule/x.py",
        "module/submodule/y.py",
        "module/submodule/z.py",
    ]
    files = [File(path, 'package', 'rootdir', hashstr='x') for path in paths]

    sorted_files = sort_list_of_files(files)

    assert expected_paths == [file.path for file in sorted_files]
