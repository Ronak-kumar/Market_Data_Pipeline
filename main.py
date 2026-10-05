from src.extraction.core.daily_data_extractor import DailyDataExtractor

if __name__ == "__main__":
    main_runner = DailyDataExtractor()
    date_map = main_runner.process()
