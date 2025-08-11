#!/usr/bin/env python3
"""
Основной модуль приложения для конвертации CSV в XML
"""

import logging
from modules.file_manager import create_file_manager, create_cli_manager
from modules.logger_manager import LoggerManager, LoggerConfig
from modules.csv_processor import create_csv_processor, create_batch_processor

def main():
    """Основная функция приложения."""
    
    # Получаем параметры из CLI
    cli_manager = create_cli_manager()
    folder_uid, csv_dir = cli_manager.get_cli_parameters()
    
    # Создаем менеджер файлов
    file_manager = create_file_manager(csv_dir)
    
    # Проверяем директорию и получаем список файлов
    csv_files = cli_manager.validate_and_list_files(file_manager)
    if not csv_files:
        return
    
    
    
    # Настройка логирования
    log_dir = file_manager.create_log_directory()
    logger_config = LoggerConfig()
    logger_manager = LoggerManager(logger_config)
    
    # Создаем пакетный процессор
    batch_processor = create_batch_processor()
    
    # Фабрика логгеров для каждого файла
    def logger_factory(csv_filename):
        log_path = file_manager.get_log_path(csv_filename)
        return logger_manager.create_logger(
            f"csv_processor_{csv_filename}",
            log_file_path=log_path,
            config=logger_config
        )
    
    # Обрабатываем файлы
    results = batch_processor.process_file_list(
        folder_uid=folder_uid,
        csv_dir=csv_dir,
        file_list=csv_files,
        logger_factory=logger_factory
    )
    
    # Подсчет результатов
    success_count = sum(1 for success in results.values() if success)
    total_count = len(results)
    
    # Вывод результатов
    print("\nРезультаты обработки:")
    for filename, success in results.items():
        status = "✅ Успешно" if success else "❌ Ошибка"
        print(f"  {filename}: {status}")
    
    # Вывод сообщения о завершении
    print("\n" + "="*50)
    print(f"🏁 Обработка завершена!")
    print(f"   Успешно: {success_count}/{total_count}")
    if success_count < total_count:
        print(f"   Ошибок: {total_count - success_count}")
    print("="*50)

if __name__ == "__main__":
    main()