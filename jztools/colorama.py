from typing import Optional
import colorama
from colorama import Fore, Back, Style

colorama.init()


def colored_text(
    text, fore_color: Optional[str] = None, back_color: Optional[str] = None
):
    return (
        (getattr(Fore, fore_color.upper()) if fore_color is not None else "")
        + (getattr(Back, back_color.upper()) if back_color is not None else "")
        + text
        + Style.RESET_ALL
    )
