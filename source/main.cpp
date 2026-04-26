#include <iostream>
#include <vector>
#include <thread>
#include <mutex>
#include <chrono>
#include <MQTTClient.h>
#include <nlohmann/json.hpp>
#include <sqlite3.h>
#include <fstream>
#include <atomic>   

#define ADDRESS "tcp://57.128.199.252:1883"
#define USER "mqttuser"
#define HASLO "baniaUCygana2137."
#define MAX_SAMPLES 50
#define NUMBER_SAMPLES_TO_SAVE_DB_FILE 10

struct Weather_Data {
    int nr_probki;
    float temp;
    float wind;
    float sunny;
};

// Zmienne globalne 
std::vector<Weather_Data> wektor_pomiarow;
std::vector<Weather_Data> ostatnie_raw;
Weather_Data ostatnia_srednia{ 0, 0, 0, 0 };


std::atomic<bool> dane_gotowe{ false };
std::atomic<int> licznik_probki{ 0 };
//atomic a volatile, atomci jest mocniejszy

std::mutex mtx;      // chroni wektor_pomiarow
std::mutex mtx_avg;  // chroni dane do wysyłki
sqlite3* db;

void init_db() {
    // Otwieramy lub tworzymy bazę danych o nazwie "weather.db"

    if (sqlite3_open("weather.db", &db)) {
        std::cerr << "Nie udalo się otworzyc bazy\n";
        return;
	} 

    const char* sql = "CREATE TABLE IF NOT EXISTS hourly_avg ("
        "id INTEGER PRIMARY KEY AUTOINCREMENT,"
        "timestamp DATETIME DEFAULT CURRENT_TIMESTAMP,"
        "nr_probki INT, temp REAL, wind REAL, sunny REAL);";
    sqlite3_exec(db, sql, 0, 0, nullptr);
}

void save_to_db(const Weather_Data& avg) {
    std::string sql = "INSERT INTO hourly_avg (nr_probki, temp, wind, sunny) VALUES (" +
        std::to_string(avg.nr_probki) + "," + std::to_string(avg.temp) + "," +
        std::to_string(avg.wind) + "," + std::to_string(avg.sunny) + ");";
    sqlite3_exec(db, sql.c_str(), 0, 0, nullptr);
}

void save_avg_to_file(const Weather_Data& avg) {

    std::ofstream file("average.txt", std::ios::app);
	//utworz, jeśli nie istnieje, i dopisz do niego

    if (file.is_open()) {
        file << "Nr probki: " << avg.nr_probki
            << ", Temp: " << avg.temp
            << ", Wind: " << avg.wind
            << ", Sunny: " << avg.sunny << "\n";
        file.close();
    }
}

Weather_Data srednia_wartosc_pomiarow(const std::vector<Weather_Data>& dane) {
    Weather_Data srednia{ 0, 0.0f, 0.0f, 0.0f };
    if (dane.empty()) return srednia;

    float s_temp = 0, s_wind = 0, s_sun = 0;

    for (const auto& p : dane) {

        s_temp += p.temp; s_wind += p.wind; s_sun += p.sunny;

    }
    srednia.temp = s_temp / dane.size();
    srednia.wind = s_wind / dane.size();
    srednia.sunny = s_sun / dane.size();
    return srednia;
}

// --- WĄTKI ---

void task_odbior() {
    MQTTClient client;
    MQTTClient_connectOptions conn_opts = MQTTClient_connectOptions_initializer;
    MQTTClient_create(&client, ADDRESS, "C2_receiver", MQTTCLIENT_PERSISTENCE_NONE, NULL);
    conn_opts.username = USER;
    conn_opts.password = HASLO;

    while (MQTTClient_connect(client, &conn_opts) != MQTTCLIENT_SUCCESS) {
        std::this_thread::sleep_for(std::chrono::seconds(2));
    }
    std::cout << "[TASK 1] Połączono z MQTT\n";
    MQTTClient_subscribe(client, "C1/weather", 1);

    while (true) {
        char* topicName = NULL;
        int topicLen;
        MQTTClient_message* message = NULL;

        if (MQTTClient_receive(client, &topicName, &topicLen, &message, 100) == MQTTCLIENT_SUCCESS && message != NULL) {

            std::string payload((char*)message->payload, message->payloadlen);

            try {
                auto j = nlohmann::json::parse(payload);
                Weather_Data p{ j["nr_probki"], j["temp"], j["wind"], j["sunny"] };

                mtx.lock();

                wektor_pomiarow.push_back(p);
                std::cout << "[TASK 1] ODBIOR: " << p.nr_probki << " | wektor: " << wektor_pomiarow.size() << std::endl;

                mtx.unlock(); 

            }
            catch (...) { std::cerr << "[TASK 1] JSON error\n"; }

            MQTTClient_freeMessage(&message);
            MQTTClient_free(topicName);
        }
    }
}

void task_przetwarzanie() {
    while (true) {

        std::vector<Weather_Data> do_obliczen;
        bool mozna_liczyc = false;

        mtx.lock();
        if (wektor_pomiarow.size() >= MAX_SAMPLES) {
            
            do_obliczen = std::move(wektor_pomiarow); 
            wektor_pomiarow.clear();                  

            mozna_liczyc = true;
        }
        mtx.unlock();

        if (mozna_liczyc) {

            Weather_Data avg = srednia_wartosc_pomiarow(do_obliczen);

            avg.nr_probki = do_obliczen.back().nr_probki;
            //ostatni numer probki 
            std::cout << "[TASK 2] Obliczono srednia dla: " << avg.nr_probki << std::endl;
            
            licznik_probki++; 
            if (licznik_probki >= NUMBER_SAMPLES_TO_SAVE_DB_FILE) {
                save_to_db(avg);
                save_avg_to_file(avg);
                licznik_probki = 0; 
            }

            mtx_avg.lock();
            ostatnia_srednia = avg;
            ostatnie_raw = do_obliczen;
            dane_gotowe = true;
            mtx_avg.unlock();
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(10)); 
    }
}
void task_wyslanie() {
    MQTTClient client;
    MQTTClient_connectOptions conn_opts = MQTTClient_connectOptions_initializer;
    MQTTClient_create(&client, ADDRESS, "C2_sender", MQTTCLIENT_PERSISTENCE_NONE, NULL);
    conn_opts.username = USER;
    conn_opts.password = HASLO;

    while (MQTTClient_connect(client, &conn_opts) != MQTTCLIENT_SUCCESS) {
        std::this_thread::sleep_for(std::chrono::seconds(2));
    }
    std::cout << "[TASK 3] Połączono sender MQTT\n";

    while (true) {
        Weather_Data avg;
        std::vector<Weather_Data> raw;
        bool wysylaj = false;

        mtx_avg.lock();
        if (dane_gotowe) {
            avg = ostatnia_srednia;
            raw = ostatnie_raw;
            dane_gotowe = false;
            wysylaj = true;
        }
        mtx_avg.unlock();

        if (wysylaj) {
            // Tworzymy JSON dla średniej (C2/avg)
            nlohmann::json j_avg = {
                {"nr_probki", avg.nr_probki},
                {"temp", avg.temp},
                {"wind", avg.wind},
                {"sunny", avg.sunny}
            };
            std::string p_avg = j_avg.dump(); // Zamiana jsona na tekst 
            MQTTClient_publish(client, "C2/avg", p_avg.length(), p_avg.c_str(), 1, 0, NULL);

            // Tworzymy JSON dla danych surowych (C2/raw) jako tablicę
            nlohmann::json j_raw_list = nlohmann::json::array();
            for (const auto& p : raw) {
                j_raw_list.push_back({
                    {"nr_probki", p.nr_probki},
                    {"temp", p.temp},
                    {"wind", p.wind},
                    {"sunny", p.sunny}
                    });
            }
            std::string p_raw = j_raw_list.dump(); // Zamiana na tekst
            MQTTClient_publish(client, "C2/raw", p_raw.length(), p_raw.c_str(), 1, 0, NULL);

            std::cout << "[TASK 3] Dane AVG i RAW wysłane na broker!" << std::endl;
        }

        std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }
}

int main() {
    init_db();
    std::thread t1(task_odbior);
    std::thread t2(task_przetwarzanie);
    std::thread t3(task_wyslanie);

    t1.join();
    t2.join();
    t3.join();

    return 0;
}