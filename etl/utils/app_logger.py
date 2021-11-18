import logging

def _get_logger(name:str, level:str="DEBUG"):
    """Util funtion to have a single logging layer over entire app.
    
    Parameters
    ---------
    
    name: str
        Name of the method (class or funtions) 
    
    level: str
        logging level for the logger.
    """
    logging.basicConfig(format="%(asctime)s:%(levelname)s - %(name)s : %(message)s", datefmt='%d/%m/%Y %H:%M:%S')
    logger = logging.getLogger(name)
    logger.setLevel(level=level)
    return logger