//
// Created by stephane bourque on 2021-03-01.
//

#ifndef UCENTRAL_USTORAGESERVICE_H
#define UCENTRAL_USTORAGESERVICE_H

#include "SubSystemServer.h"

#include "Poco/Data/Session.h"
#include "Poco/Data/SQLite/SQLite.h"

#include "RESTAPI_Objects.h"

namespace uCentral::Storage {

    class Service : public SubSystemServer {

    public:
        Service() noexcept;

        int Start() override;

        void Stop() override;

        bool AddStatisticsData(std::string &SerialNUmber, uint64_t CfgUUID, std::string &NewStats);
        bool GetStatisticsData(std::string &SerialNUmber, std::string & FromDate, std::string & ToDate, uint64_t Offset, uint64_t HowMany,
                               std::vector<uCentralStatistics> &Stats);

        bool UpdateDeviceConfiguration(std::string &SerialNUmber, std::string &Configuration);
        bool CreateDevice(uCentralDevice &);
        bool GetDevice(std::string &SerialNUmber, uCentralDevice &);
        uint64_t GetDevices(uint64_t From, uint64_t Howmany, std::vector<uCentralDevice> &Devices);
        bool DeleteDevice(std::string &SerialNUmber);
        bool UpdateDevice(uCentralDevice &);

        bool ExistingConfiguration(std::string &SerialNumber, uint64_t CurrentConfig, std::string &NewConfig, uint64_t &);

        bool UpdateDeviceCapabilities(std::string &SerialNUmber, std::string &State);
        bool GetDeviceCapabilities(std::string &SerialNUmber, uCentralCapabilities &);

        static Service *instance() {
            if (instance_ == nullptr) {
                instance_ = new Service;
            }
            return instance_;
        }

    private:
        static Service *instance_;
        std::shared_ptr<Poco::Data::Session> session_;
        std::mutex mutex_;
    };

};  // namespace

#endif //UCENTRAL_USTORAGESERVICE_H
