with open("tests/test_install_bootstrap_portable.py", "r") as f:
    data = f.read()
data = data.replace("'@tanstack/react-query': '^5.100.10'", "'@tanstack/react-query': '^5.101.4'")
with open("tests/test_install_bootstrap_portable.py", "w") as f:
    f.write(data)
