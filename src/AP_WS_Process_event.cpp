//
// Created by stephane bourque on 2023-01-22.
//
#include "AP_WS_Connection.h"
#include "StorageService.h"
#include "Daemon.h"

#include "fmt/format.h"
#include "framework/KafkaManager.h"
#include "framework/ow_constants.h"

namespace OpenWifi {
	void AP_WS_Connection::Process_event(Poco::JSON::Object::Ptr ParamsObj) {
		if (!State_.Connected) {
			poco_warning(Logger_,
						 fmt::format("INVALID-PROTOCOL({}): Device '{}' is not following protocol",
									 CId_, CN_));
			Errors_++;
			return;
		}
		poco_trace(Logger_, fmt::format("Event data received for {}", SerialNumber_));

		try {
			if (ParamsObj->has(uCentralProtocol::SERIAL) &&
				ParamsObj->has(uCentralProtocol::DATA)) {

                    auto Data = ParamsObj->getObject(uCentralProtocol::DATA);
                    auto Event = Data->getArray("event");
                    uint64_t Timestamp = 0;
                    Poco::JSON::Object::Ptr EventDetails;

                    if (Event->size() == 2) {
                        Timestamp = Event->getElement<std::uint64_t>(0);
                        EventDetails = Event->getObject(1);
                    } else if (Event->size() == 1){
                        EventDetails = Event->getObject(0);
                    }
                    auto EventType = EventDetails->get("type").extract<std::string>();

                    if (EventType == "firmware_upgrade_status") {

                        auto EventPayload = EventDetails->getObject("payload");
                        auto GetUpgradeType = EventPayload->get("operation").extract<std::string>();
                        std::string GetUpgradeTypePercentage;
                        std::string UpdateStatus;
                        if (GetUpgradeType == "download") {
                            GetUpgradeTypePercentage = EventPayload->get("percentage").toString();
                            UpdateStatus = GetUpgradeType + "_" + GetUpgradeTypePercentage + "%";
                        } else {
                            UpdateStatus = GetUpgradeType;
                        }

                        GWObjects::Device       DeviceInfo;
                        StorageService()->GetDevice(SerialNumber_, DeviceInfo);

                        if (Daemon()->IdentifyDevice(DeviceInfo.DeviceType) == "SWITCH") {
                            GWObjects::CommandDetails Cmd;
                            GWObjects::CommandDetails RCommand;
                            Cmd.SerialNumber = SerialNumber_;
                            Cmd.SubmittedBy = uCentralProtocol::SUBMITTED_BY_SYSTEM;
                            Cmd.Command = uCentralProtocol::UPGRADE;
                            if (GetUpgradeType == "success") {
                                Cmd.Status = "completed";
                            } else {
                                Cmd.Status = UpdateStatus;
                            }
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

                            if (StorageService()->UpdateLastCommandStatus(Cmd, RCommand)) {
                                poco_information(Logger_, fmt::format("UPGRADE for serial : {} is in progress with {}",SerialNumber_, UpdateStatus));
                            } else {
                                poco_information(Logger_, fmt::format("Failed to parse Updated Status for serial {}",SerialNumber_));
                            }
                        }
                    }
                }

				if (KafkaManager()->Enabled()) {
					auto Data = ParamsObj->getObject(uCentralProtocol::DATA);
					auto Event = Data->getArray("event");
					auto EventTimeStamp = Event->getElement<std::uint64_t>(0);
					auto EventDetails = Event->getObject(1);
					auto EventType = EventDetails->get("type").extract<std::string>();
					auto EventPayload = EventDetails->getObject("payload");

					Poco::JSON::Object FullEvent;
					FullEvent.set("type", EventType);
					FullEvent.set("timestamp", EventTimeStamp);
					FullEvent.set("payload", EventPayload);

					std::ostringstream OS;
					FullEvent.stringify(OS);
					KafkaManager()->PostMessage(KafkaTopics::DEVICE_EVENT_QUEUE, SerialNumber_,
												OS.str());
				}
		} catch (const Poco::Exception &E) {
			Logger_.log(E);
		} catch (...) {
		}
	}
} // namespace OpenWifi
