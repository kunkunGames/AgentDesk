with open('tests/test_fast_check_ci_wiring.py', 'r') as f:
    content = f.read()

content = content.replace(
    'actions/checkout@11d5960a326750d5838078e36cf38b85af677262\\n',
    'actions/checkout@11d5960a326750d5838078e36cf38b85af677262 # v4\\n'
)

with open('tests/test_fast_check_ci_wiring.py', 'w') as f:
    f.write(content)
