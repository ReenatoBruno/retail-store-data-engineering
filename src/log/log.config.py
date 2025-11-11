import logging 
import os 

def setup_logging(log_file: str = 'project.log'):
    # CREATE LOGS DIRECTORY IF IT DOESN'T EXIST
    os.makedirs('logs', exist_ok=True)
    log_path = os.path.join('logs', log_file)

    # BASIC CONFIGURATION
    logging.basicConfig(
        level=logging.debug,
        format='%(astime)s - %(levelname)s - %(message)s', 
        datefmt="%Y-%m-%d %H:%M:%S",
         handlers=[
            logging.FileHandler(log_path),  # SAVE LOG TO FILE
            logging.StreamHandler()         # SHOW OUTPUT ON CONSOLE
        ]
    )

    logging.info('Logging initialized successfully!')


