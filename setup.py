import os
from setuptools import setup, find_packages

# Функция для чтения содержимого файла README.md
def read_readme():
    readme_path = os.path.join(os.path.dirname(__file__), 'README.md')
    if os.path.exists(readme_path):
        with open(readme_path, 'r', encoding='utf-8') as f:
            return f.read()
    return "Arkham API Client Library" # Запасной вариант, если README.md нет

# Чтение зависимостей из requirements.txt
# Это полезно, если у вас уже есть requirements.txt для ядра библиотеки
# Если requirements.txt также содержит зависимости для Streamlit или тестов,
# то лучше перечислить основные зависимости библиотеки здесь вручную.
# Я пока закомментирую это и предложу перечислить вручную.
# def read_requirements():
#     req_path = os.path.join(os.path.dirname(__file__), 'requirements.txt')
#     if os.path.exists(req_path):
#         with open(req_path, 'r', encoding='utf-8') as f:
#             return [line.strip() for line in f if line.strip() and not line.startswith('#')]
#     return []

setup(
    name='arkham_client',  # Имя вашей библиотеки для PyPI или для pip install
                                 # Выберите что-то уникальное, например, с вашим префиксом.
                                 # Например: 'yury_arkham_client' или 'arkham_alert_sdk'
    version='0.1.0',             # Начальная версия вашей библиотеки
    author='VYV', # Замените на ваше имя/ник
    author_email='yury.valau@gmail.com', # Замените на ваш email (опционально)
    description='Клиентская библиотека для взаимодействия с Arkham API и мониторинга транзакций.',
    long_description=read_readme(),
    long_description_content_type='text/markdown',
    url='https://github.com/vy-bewhale/ArkhamAlert', # ИЗМЕНЕНО
    packages=find_packages(include=['arkham', 'arkham.*']), # Находит ваш пакет 'arkham' и его подмодули
                                                          # Это означает, что после установки можно будет делать
                                                          # from arkham.some_module import something
    classifiers=[ # Классификаторы для PyPI (опционально, но полезно)
        'Development Status :: 3 - Alpha', # Статус разработки
        'Intended Audience :: Developers',
        'License :: OSI Approved :: MIT License', # Выберите лицензию или удалите, если не определились
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.8',
        'Programming Language :: Python :: 3.9',
        'Programming Language :: Python :: 3.10',
        'Programming Language :: Python :: 3.11',
        'Operating System :: OS Independent',
    ],
    python_requires='>=3.8', # Минимальная требуемая версия Python
    install_requires=[       # Список основных зависимостей ВАШЕЙ БИБЛИОТЕКИ
        'pandas>=1.0',       # Укажите актуальные версии, если знаете
        'python-dotenv>=0.15',
        'requests>=2.20',    # Добавьте 'requests' или другую HTTP библиотеку, если она используется
                             # в вашей папке arkham/ для API запросов.
        # 'aiohttp>=3.0',    # Если используете асинхронные запросы
        # ...другие критичные зависимости для работы arkham/...
    ],
    # entry_points={ # Если у вас есть консольные скрипты в библиотеке (опционально)
    #     'console_scripts': [
    #         'arkham-cli=arkham.cli:main',
    #     ],
    # },
    project_urls={ # Дополнительные ссылки (опционально)
        'Bug Reports': 'https://github.com/vy-bewhale/ArkhamAlert/issues',
        'Source': 'https://github.com/vy-bewhale/ArkhamAlert/',
    },
) 