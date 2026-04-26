# Programowanie systemów czasu rzeczywistego 

Projekt realizuje zadanie na studia z przedmiotu PSCR.  
Zadanie polegało na implementacji poniższego schematu. Za każde stanowisko odpowiadała jedna osoba.  
System miał być zaimplementowany w chmurze i działać co najmniej przez tydzień.  

<img width="1724" height="488" alt="image" src="https://github.com/user-attachments/assets/9d4382a5-f3f7-4ff8-aa0d-3799ac467fb9" />

Komunikacja pomiędzy stanowiskami odbyła się za pomocą protokołu MQTT.  
Jako chmura został wybrany program Microsoft Azure.  

Moje stanowisko było stanowiskiem C2, gdzie musiałem odbierać dane z MQTT, przetworzyć je, korzystając z mechanizmu mutex, oraz wysłać na stanowisko C4.  
Również, korzystając z SQLite, zapisywałem do bazy danych pomiary w celu późniejszego pokazania logów z tygodnia.  

Postanowiłem w celach naukowych również samemu zaimplementować stanowisko C1 oraz C4, gdzie korzystałem z FastAPI oraz Data Cloud.  

## Środowisko uruchomieniowe
Program został uruchomiony w środowisku Linux, z wykorzystaniem konteneryzacji (Docker) oraz maszyny wirtualnej w chmurze Microsoft Azure.

## Technologie
- Linux  
- Docker  
- MQTT  
- FastAPI  
- Cloud Database  

## Mechanizmy systemowe
- wielowątkowość  
- mutex (synchronizacja sekcji krytycznych)  
- semaphore (zarządzanie dostępem do zasobów)  
- kolejki FIFO  
- pipeline przetwarzania danych  
