import logging

logger = logging.getLogger('hobgoblin')

if not logger.isEnabledFor(1):
    log_file = logging.FileHandler('C:\\tmp\\hobgoblin.log', 'w')
    log_file.setFormatter(logging.Formatter("""%(levelname)s:%(name)s:%(funcName)s
    %(message)s
    :"""))
    logger.addHandler(log_file)
    logger.setLevel(1)

    logger.debug("imported")
