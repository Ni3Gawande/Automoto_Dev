import logging

def setup_logger():
    # Configure the logging
    logging.basicConfig(
        filename=r'C:\Users\Anshu\Desktop\folder\ETL\Capstone\Logs\etlprocess.log',  # Log file path
        filemode='a',  # Append mode
        format='%(asctime)s - %(levelname)s - %(message)s',  # Log format
        level=logging.INFO  # Set the logging level to INFO
    )
    logger = logging.getLogger(__name__)  # Create a logger instance
    return logger

# Example usage
# if __name__ == "__main__":
#     logger = setup_logger()
#     logger.info("Logging setup is complete.")
#     logger.warning("This is a warning message.")
#     logger.error("This is an error message.")
