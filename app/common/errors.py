class TossError(Exception):
    """Toss 스크래핑 중 발생하는 주요 에러."""

    def __init__(self, message: str, code: int = 500):
        super().__init__(message)
        self.code = code
        self.message = message

    def __str__(self):
        return f"[Code {self.code}] {self.message}"

    def to_dict(self):
        """에러를 dict 형태로 반환"""
        return {"code": self.code, "message": self.message}


class NaverError(Exception):
    """Naver 스크래핑 중 발생하는 주요 에러."""

    def __init__(self, message: str, code: int = 500):
        super().__init__(message)
        self.code = code
        self.message = message

    def __str__(self):
        return f"[Code {self.code}] {self.message}"

    def to_dict(self):
        """에러를 dict 형태로 반환"""
        return {"code": self.code, "message": self.message}
