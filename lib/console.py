class Color:
    PURPLE = "\033[95m"
    CYAN = "\033[96m"
    DARK_CYAN = "\033[36m"
    BLUE = "\033[94m"
    GREEN = "\033[92m"
    YELLOW = "\033[93m"
    ORANGE = "\033[33m"
    RED = "\033[91m"
    BOLD = "\033[1m"
    UNDERLINE = "\033[4m"
    END = "\033[0m"


class Console:
    """
    Defines static logging methods for use in the main script.
    """
    @staticmethod
    def log(message="", end='\n', flush=False):
        print(message, end=end, flush=flush)

    @staticmethod
    def error(message, bold=False):
        Console.log(f"{Color.BOLD if bold else ''}{Color.RED}{message}{Color.END}")

    @staticmethod
    def warn(message, bold=False):
        Console.log(f"{Color.BOLD if bold else ''}{Color.ORANGE}{message}{Color.END}")

    @staticmethod
    def info(message, bold=False):
        Console.log(f"{Color.BOLD if bold else ''}{Color.DARK_CYAN}{message}{Color.END}")

    # Colors
    @staticmethod
    def purple(message, bold=False, underline=False):
        Console.log(f"{Color.BOLD if bold else ''}"
                    f"{Color.UNDERLINE if underline else ''}"
                    f"{Color.PURPLE}{message}{Color.END}")

    @staticmethod
    def cyan(message, bold=False, underline=False):
        Console.log(f"{Color.BOLD if bold else ''}"
                    f"{Color.UNDERLINE if underline else ''}"
                    f"{Color.CYAN}{message}{Color.END}")

    @staticmethod
    def dark_cyan(message, bold=False, underline=False):
        Console.log(f"{Color.BOLD if bold else ''}"
                    f"{Color.UNDERLINE if underline else ''}"
                    f"{Color.DARK_CYAN}{message}{Color.END}")

    @staticmethod
    def blue(message, bold=False, underline=False):
        Console.log(f"{Color.BOLD if bold else ''}"
                    f"{Color.UNDERLINE if underline else ''}"
                    f"{Color.BLUE}{message}{Color.END}")

    @staticmethod
    def green(message, bold=False, underline=False):
        Console.log(f"{Color.BOLD if bold else ''}"
                    f"{Color.UNDERLINE if underline else ''}"
                    f"{Color.GREEN}{message}{Color.END}")

    @staticmethod
    def yellow(message, bold=False, underline=False):
        Console.log(f"{Color.BOLD if bold else ''}"
                    f"{Color.UNDERLINE if underline else ''}"
                    f"{Color.YELLOW}{message}{Color.END}")

    @staticmethod
    def orange(message, bold=False, underline=False):
        Console.log(f"{Color.BOLD if bold else ''}"
                    f"{Color.UNDERLINE if underline else ''}"
                    f"{Color.ORANGE}{message}{Color.END}")

    @staticmethod
    def red(message, bold=False, underline=False):
        Console.log(f"{Color.BOLD if bold else ''}"
                    f"{Color.UNDERLINE if underline else ''}"
                    f"{Color.RED}{message}{Color.END}")

    # Format
    @staticmethod
    def bold(message, underline=False):
        Console.log(f"{Color.BOLD}{Color.UNDERLINE if underline else ''}{message}{Color.END}")

    @staticmethod
    def underline(message, bold=False):
        Console.log(f"{Color.UNDERLINE}{Color.BOLD if bold else ''}{message}{Color.END}")
