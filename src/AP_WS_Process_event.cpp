//
#include "AP_WS_Connection.h"
#include "StorageService.h"
#include "Daemon.h"

#include "framework/ow_constants.h"
#include "fmt/format.h"
#include "framework/KafkaManager.h"

namespace OpenWifi {
        void AP_WS_Connection::Process_event(Poco::JSON::Object::Ptr ParamsObj) {
                                Poco::JSON::Stringifier Stringify1;
                                std::ostringstream OS1;
                                Stringify1.condense(ParamsObj, OS1);
                                poco_information(Logger_, fmt::format("first karthick Event data received for {} {}", SerialNumber_,OS1.str()));
                if (!State_.Connected) {
                        poco_warning(
                                Logger_,
                                fmt::format("INVALID-PROTOCOL({}): Device '{}' is not following protocol", CId_, CN_));
                        Errors_++;
                        return;
                }
                poco_trace(Logger_, fmt::format("Event data received for {}", SerialNumber_));


                if (ParamsObj->has(uCentralProtocol::SERIAL) &&
                    ParamsObj->has(uCentralProtocol::DATA)) {

                    auto Data = ParamsObj->getObject(uCentralProtocol::DATA);
                    poco_information(Logger_, fmt::format("karthick get data"));
                    auto Event = Data->getArray("event");
                    auto Timestamp = Event->getElement<std::uint64_t>(0);
                    auto EventDetails = Event->getObject(1);
                    poco_information(Logger_, fmt::format("karthick get get_event"));
                    auto EventType = EventDetails->get("type").extract<std::string>();
                    poco_information(Logger_, fmt::format("karthick get event_type {}", EventType));
                    auto EventPayload = EventDetails->getObject("payload");
                    poco_information(Logger_, fmt::format("karthick get get_payload"));
                    auto GetUpgradeType = EventPayload->get("operation").extract<std::string>();
                    poco_information(Logger_, fmt::format("karthick get get_operation {}", GetUpgradeType));
                    std::string GetUpgradeTypePercentage;
                    if (GetUpgradeType == "download") {
                    GetUpgradeTypePercentage = EventPayload->get("percentage").toString();
                    poco_information(Logger_, fmt::format("karthick get get_percentage {}", GetUpgradeTypePercentage));
		    }

                    if (EventType == "firmware_upgrade_status") {
                        poco_information(Logger_, fmt::format("karthick inside if "));
                        GWObjects::Device       DeviceInfo;
                        StorageService()->GetDevice(SerialNumber_, DeviceInfo);

                        if (Daemon()->IdentifyDevice(DeviceInfo.DeviceType) == "SWITCH" &&
                            DeviceInfo.FWupgradeInprogress == true) {
                            GWObjects::CommandDetails Cmd;
                            Cmd.SerialNumber = SerialNumber_;
                            //Cmd.UUID = MicroServiceCreateUUID();
                            Cmd.SubmittedBy = uCentralProtocol::SUBMITTED_BY_SYSTEM;
                            Cmd.Command = uCentralProtocol::UPGRADE;
                            if (Timestamp > 0) {
                                Cmd.Completed = Timestamp;
                            }else {
                                Cmd.Completed = Utils::Now();
                            }

                            Poco::JSON::Object Params;
                            Params.set(uCentralProtocol::SERIAL, SerialNumber_);
                            Params.set(uCentralProtocol::UUID, Cmd.UUID);
                            Params.set(uCentralProtocol::WHEN, Cmd.Completed);
                            Params.set("operation",GetUpgradeType);
                            Params.set("Percentage",GetUpgradeTypePercentage);
                            std::stringstream ParamStream;
                            Params.stringify(ParamStream);
                            Cmd.Details = ParamStream.str();

                            poco_information(Logger_, fmt::format("karthick inside second if "));
                            Cmd.UpgradeType = GetUpgradeType;
                            //Cmd.Status = check_operation;

                            if (GetUpgradeType == "download") {
                                Cmd.UUID = DeviceInfo.FWDownloadUUID;
                                poco_information(Logger_, fmt::format("karthick getupgradetype download "));
                                StorageService()->AddCommand(Cmd.SerialNumber, Cmd, Storage::CommandExecutionType::COMMAND_EXECUTING);
                            } else if (GetUpgradeType == "install" || GetUpgradeType == "inatall"){
                                Cmd.UUID = DeviceInfo.FWInstallUUID;
                                poco_information(Logger_, fmt::format("karthick getupgradetype install "));
                                StorageService()->AddCommand(Cmd.SerialNumber, Cmd, Storage::CommandExecutionType::COMMAND_EXECUTING);
                            } else {
                                poco_information(Logger_, fmt::format("karthick getupgradetype completed "));
                                Cmd.UUID = MicroServiceCreateUUID();
                                StorageService()->AddCommand(Cmd.SerialNumber, Cmd, Storage::CommandExecutionType::COMMAND_COMPLETED);
                                DeviceInfo.FWUpgradeInprogress = false;
                                DeviceInfo.FWInstallUUID = "";
                                DeviceInfo.FWDownloadUUID = "";
                                StorageService()->UpdateDevice(DeviceInfo);
                            }
                        }
                    }
                }

                if (ParamsObj->has(uCentralProtocol::SERIAL) && ParamsObj->has(uCentralProtocol::DATA)) {
                        if (KafkaManager()->Enabled()) {
                                auto Data = ParamsObj->get(uCentralProtocol::DATA);
                                Poco::JSON::Stringifier Stringify;
                                std::ostringstream OS;
                                Stringify.condense(ParamsObj, OS);
                                KafkaManager()->PostMessage(KafkaTopics::DEVICE_EVENT_QUEUE, SerialNumber_, OS.str());
                        }
                }
        }
}
