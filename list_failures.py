import unittest
import sys

def main():
    suite = unittest.defaultTestLoader.loadTestsFromName("tests.test_fast_check_ci_wiring")
    result = unittest.TextTestRunner(verbosity=2, stream=sys.stderr).run(suite)
    if not result.wasSuccessful():
        print("\n\nFAILURES:")
        for failure in result.failures:
            print(failure[0])
            print(failure[1])
        print("\n\nERRORS:")
        for error in result.errors:
            print(error[0])
            print(error[1])
        sys.exit(1)

if __name__ == "__main__":
    main()
