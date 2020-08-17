package dashclient

//EVENT NAMES
const (
	EvtNewMPD       = "NEW_MPD"        //Received new MPD file - No Fields
	EvtUpdMPD       = "UPD_MPD"        //Received MPD update - Update Delta
	EvtMPDFetchFail = "MPD_FETCH_FAIL" //Fetch failed

	EvtClientBitRateSelect   = "CLIENT_BITRATE_SELECTED"  //Bitrate is selected by client - Bitrate
	EvtClientContentAbsent   = "CLIENT_DRAINED"           //Client Drained - Duration of data missing
	EvtClientAllRepsFiltered = "CLIENT_ALL_REPS_FILTERED" //All Representations filtered

	EvtMp4TimeScaleAltered  = "MP4_TIMESCALE_MODIFIED" //TimeScale modified
	EvtMp4TrackIDAltered    = "MP4_TRACKID_MODIFIED"   //TrackID modified
	EvtMp4NoBoxTime         = "MP4_NO_BOX_TIME"        //Box Time cannot be determined
	EvtMp4SequenceMissed    = "MP4_Sequence_Missed"    //Box Time cannot be determined
	EvtMp4DurationGapFilled = "MP4_DurationGap_Filled" //Filled duration gap
)
