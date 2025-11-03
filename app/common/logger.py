# app/common/logger.py

import logging
import logging.handlers
import os
import re
import time
from typing import Literal


class ParallelTimedRotatingFileHandler(logging.handlers.TimedRotatingFileHandler):
    def __init__(
        self,
        filename,
        when="h",
        interval=1,
        backupCount=0,
        encoding=None,
        delay=False,
        utc=False,
        postfix=".log",
    ):
        self.origFileName = filename
        self.when = when.upper()
        self.backupCount = backupCount
        self.utc = utc
        self.postfix = postfix
        self.encoding = encoding
        self.atTime = None

        if self.when == "S":
            self.interval = 1
            self.suffix = "%Y-%m-%d_%H-%M-%S"
            self.extMatch = r"^\d{4}-\d{2}-\d{2}_\d{2}-\d{2}-\d{2}$"
        elif self.when == "M":
            self.interval = 60
            self.suffix = "%Y-%m-%d_%H-%M"
            self.extMatch = r"^\d{4}-\d{2}-\d{2}_\d{2}-\d{2}$"
        elif self.when == "H":
            self.interval = 60 * 60
            self.suffix = "%Y-%m-%d_%H"
            self.extMatch = r"^\d{4}-\d{2}-\d{2}_\d{2}$"
        elif self.when == "D" or self.when == "MIDNIGHT":
            self.interval = 60 * 60 * 24
            self.suffix = "%Y-%m-%d"
            self.extMatch = r"^\d{4}-\d{2}-\d{2}$"
        elif self.when.startswith("W"):
            self.interval = 60 * 60 * 24 * 7
            if len(self.when) != 2:
                raise ValueError(
                    "You must specify a day for weekly rollover from 0 to 6 (0 is Monday): %s"
                    % self.when
                )
            if self.when[1] < "0" or self.when[1] > "6":
                raise ValueError(
                    "Invalid day specified for weekly rollover: %s" % self.when
                )
            self.dayOfWeek = int(self.when[1])
            self.suffix = "%Y-%m-%d"
            self.extMatch = r"^\d{4}-\d{2}-\d{2}$"
        else:
            raise ValueError("Invalid rollover interval specified: %s" % self.when)

        currenttime = int(time.time())
        logging.handlers.BaseRotatingHandler.__init__(
            self, self.calculateFileName(currenttime), "a", encoding, delay
        )

        self.extMatch = re.compile(self.extMatch)
        self.interval = self.interval * interval
        self.rolloverAt = self.computeRollover(currenttime)

    def calculateFileName(self, currenttime):
        timeTuple = (
            time.gmtime(currenttime) if self.utc else time.localtime(currenttime)
        )
        return (
            self.origFileName
            + "-"
            + time.strftime(self.suffix, timeTuple)
            + self.postfix
        )

    def getFilesToDelete(self, newFileName):
        dirName, fName = os.path.split(self.origFileName)
        dName, newFileName = os.path.split(newFileName)

        fileNames = os.listdir(dirName)
        result = []

        prefix = os.path.basename(fName) + "-"
        postfix = self.postfix
        prelen = len(prefix)
        postlen = len(postfix)

        for fileName in fileNames:
            if (
                fileName.startswith(os.path.basename(fName))
                and fileName.endswith(postfix)
                and fileName != newFileName
            ):
                # Extract the timestamp part for validation
                timestamp_part = fileName[
                    len(os.path.basename(fName)) + 1 : -len(postfix)
                ]
                if self.extMatch.match(timestamp_part):
                    result.append(os.path.join(dirName, fileName))

        result.sort()
        if len(result) < self.backupCount:
            result = []
        else:
            result = result[: len(result) - self.backupCount]
        return result

    def doRollover(self):
        if self.stream:
            self.stream.close()
            self.stream = None

        currentTime = self.rolloverAt
        newFileName = self.calculateFileName(currentTime)

        if not os.path.exists(os.path.dirname(newFileName)):
            try:
                os.makedirs(os.path.dirname(newFileName))
            except OSError:
                pass
        self.baseFilename = os.path.abspath(newFileName)
        self.stream = self._open()

        if self.backupCount > 0:
            for s in self.getFilesToDelete(newFileName):
                try:
                    os.remove(s)
                except OSError:
                    pass

        newRolloverAt = self.computeRollover(currentTime)
        while newRolloverAt <= currentTime:
            newRolloverAt += self.interval

        if (self.when == "MIDNIGHT" or self.when.startswith("W")) and not self.utc:
            dstNow = time.localtime(currentTime)[-1]
            dstAtRollover = time.localtime(newRolloverAt)[-1]
            if dstNow != dstAtRollover:
                if not dstNow:
                    newRolloverAt = newRolloverAt - 3600
                else:
                    newRolloverAt = newRolloverAt + 3600
        self.rolloverAt = newRolloverAt


DeliveryService = Literal["baemin", "coupangeats", "yogiyo"]


def setup_loggers():
    # --- 1. Main 로거 설정 ---
    main_log_dir = "log/dev"
    os.makedirs(main_log_dir, exist_ok=True)
    main_logger = logging.getLogger("dev")
    main_logger.setLevel(logging.INFO)

    if not main_logger.handlers:
        formatter = logging.Formatter(
            "[%(levelname)s] %(asctime)s - [%(route)s] - %(message)s",
            defaults={"route": "system"},
        )
        handler = ParallelTimedRotatingFileHandler(
            filename=f"{main_log_dir}/dev",
            when="midnight",
            interval=1,
            backupCount=30,
            encoding="utf-8",
        )
        handler.setFormatter(formatter)
        main_logger.addHandler(handler)
        main_logger.propagate = False

    # --- 2. 서비스별 로거 설정 ---
    services: list[DeliveryService] = ["baemin", "coupangeats", "yogiyo"]
    for service_name in services:
        log_dir = f"log/{service_name}"
        os.makedirs(log_dir, exist_ok=True)
        logger = logging.getLogger(service_name)
        logger.setLevel(logging.INFO)
        if not logger.handlers:
            formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
            handler = ParallelTimedRotatingFileHandler(
                filename=f"{log_dir}/{service_name}",
                when="midnight",
                interval=1,
                backupCount=30,
                encoding="utf-8",
            )
            handler.setFormatter(formatter)
            logger.addHandler(handler)
            logger.propagate = False


setup_loggers()

main_logger = logging.getLogger("system")
baemin_logger = logging.getLogger("toss")
