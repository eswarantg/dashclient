package dashclient

import (
	"context"
	"crypto/md5"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net/http"
	"runtime"
	"sync"
	"time"

	"github.com/eswarantg/dashreader"
	"github.com/eswarantg/statzagg"

	"github.com/eswarantg/contentconsumer"
	"github.com/eswarantg/mp4box"
	"github.com/tcnksm/go-httpstat"
)

type moofMdatPair struct {
	duration time.Duration
	moof     mp4box.Box
}

//ZERODuration - ZERO Duration
const ZERODuration time.Duration = 0 * time.Second

//Mp4Source -
type Mp4Source struct {
	//External
	Timeout             time.Duration     //Timeout - HTTP client Timeout
	StatzAgg            statzagg.StatzAgg //statistics aggregator
	DefaultMoofDuration time.Duration     //duration if not available in the stream

	//Internal
	id                  string                   //id - for identification
	urlChan             chan dashreader.ChunkURL //urlChan - Input Channel
	isURLChanOpen       bool                     //isURLChanOpen - Channel open?
	urlChanMutex        sync.Mutex               //Mutex gaurding the channel
	timeScale           *uint32                  //Reference timeScale - last read
	trackID             *uint32                  //Reference trackID - last read
	sequenceNo          *uint32                  //Reference sequenceNo - last sequenceNo
	baseMediaDecodetime *uint64                  //Reference baseMediaDecodetime - last baseMediaDecodetime
	unSentBoxes         *[]mp4box.Box            //moofBoxes held for determing timing
	lastMoof            moofMdatPair             //buffer for pairing
	inCounter           uint64                   //counter of input boxes
	outCounter          uint64                   //counter of input boxes
	durCumul            time.Duration            //cumulative Duration of content in url
}

//NewMp4Source - Create new Mp4Source object
func NewMp4Source(id string) *Mp4Source {
	ret := &Mp4Source{}
	ret.id = id
	ret.Timeout = 10 * time.Second
	ret.urlChan = make(chan dashreader.ChunkURL, 20)
	ret.isURLChanOpen = true
	ret.DefaultMoofDuration = 2 * time.Second
	ret.resetReference()
	return ret
}

func (m *Mp4Source) resetReference() {
	m.timeScale = nil
	m.trackID = nil
	m.sequenceNo = nil
	m.baseMediaDecodetime = nil
	m.unSentBoxes = nil
}

//recordTimeScale
func (m *Mp4Source) recordTimeScale(timescale *uint32) (updated bool, oldValue *uint32) {
	return checkAndUpdateUint32(&m.timeScale, timescale)
}

//recordTrackID
func (m *Mp4Source) recordTrackID(trackID *uint32) (updated bool, oldValue *uint32) {
	return checkAndUpdateUint32(&m.trackID, trackID)
}

//sequenceNo
func (m *Mp4Source) recordSequenceNo(sequenceNo *uint32) (updated bool, oldValue *uint32) {
	return checkAndUpdateUint32(&m.sequenceNo, sequenceNo)
}

//baseMediaDecodetime
func (m *Mp4Source) recordBaseMediaDecodetime(baseMediaDecodetime *uint64) (updated bool, oldValue *uint64) {
	return checkAndUpdateUint64(&m.baseMediaDecodetime, baseMediaDecodetime)
}

//Channel - Returns the channel to write urls to
func (m *Mp4Source) Channel() chan<- dashreader.ChunkURL {
	m.urlChanMutex.Lock()
	defer m.urlChanMutex.Unlock()
	if m.isURLChanOpen {
		return m.urlChan
	}
	return nil
}

//CloseChannel - Close the channel
func (m *Mp4Source) CloseChannel() {
	m.urlChanMutex.Lock()
	defer m.urlChanMutex.Unlock()
	if !m.isURLChanOpen {
		return
	}
	close(m.urlChan)
	m.urlChan = nil
	m.isURLChanOpen = false
}

//Run - Dequeue URLs and Fetch content, till input chan is closed
func (m *Mp4Source) Run(ctx context.Context, wg *sync.WaitGroup, writeChan chan<- contentconsumer.TimedContentPtr) {
	log.Printf("Mp4Source::Run %v Starting...", m.id)
	defer log.Printf("Mp4Source::Run %v Exiting...", m.id)
	if wg != nil {
		defer wg.Done()
	}
	for m.urlChan != nil {
		select {
		case <-ctx.Done():
			m.CloseChannel()
			break
		case mp4url, ok := <-m.urlChan:
			if !ok {
				m.CloseChannel()
				break
			}
			//handle url
			m.durCumul = 0 * time.Second
			statz := m.getMp4(ctx, mp4url, writeChan)
			if m.StatzAgg != nil {
				m.StatzAgg.PostHTTPClientStats(ctx, statz)
			}
			if mp4url.Duration > m.durCumul {
				dummybox := &mp4box.FreeSpaceBox{}
				diff := mp4url.Duration - m.durCumul
				values := make([]interface{}, 1)
				values[0] = diff
				if m.StatzAgg != nil {
					m.StatzAgg.PostEventStats(ctx, &statzagg.EventStats{
						EventClock: time.Now(),
						ID:         m.id,
						Name:       EvtMp4DurationGapFilled,
						Values:     values,
					})
				}
				m.writeBoxToChannel(diff, dummybox, writeChan)
				m.durCumul = 0
			}
		}
	}
}

//handleBox - Record the metadata info from boxes retrieved
func (m *Mp4Source) handleBox(ctx context.Context, box mp4box.Box) error {
	var err error
	if box == nil {
		return err
	}
	switch box.Boxtype() {
	case "ftyp":
		//RESET
		m.resetReference()
	case "moov":
		var moovbox *mp4box.MovieBox
		moovbox, err = box.GetMovieBox()
		if err == nil && moovbox != nil {
			_, timescale, trackID := moovbox.Summary()
			_, oldTimeScale := m.recordTimeScale(timescale)
			if oldTimeScale != nil {
				if m.StatzAgg != nil {
					m.StatzAgg.PostEventStats(ctx, &statzagg.EventStats{
						EventClock: time.Now(),
						ID:         m.id,
						Name:       EvtMp4TimeScaleAltered,
					})
				}
				log.Printf("WARN !! TimeScale Updated!!")
			}
			_, oldTrackID := m.recordTrackID(trackID)
			if oldTrackID != nil {
				log.Printf("WARN !! TrackID Updated!!")
			}
		}
	case "sidx":
		var sidxbox *mp4box.SegmentIndexBox
		sidxbox, err = box.GetSegmentIndexBox()
		if err == nil && sidxbox != nil {
			ts := sidxbox.TimeScale()
			_, oldTimeScale := m.recordTimeScale(&ts)
			if oldTimeScale != nil {
				if m.StatzAgg != nil {
					m.StatzAgg.PostEventStats(ctx, &statzagg.EventStats{
						EventClock: time.Now(),
						ID:         m.id,
						Name:       EvtMp4TimeScaleAltered,
					})
				}
				log.Printf("WARN !! TimeScale Updated!!")
			}
		}
	}
	return err
}

//writeBoxesToChannel - Write an array of box which make the duration into channel
func (m *Mp4Source) writeBoxesToChannel(duration time.Duration, boxes []mp4box.Box, writeChan chan<- contentconsumer.TimedContentPtr) {
	m.outCounter++
	//for _, box := range boxes {
	//	log.Printf("%v %v %v %v Write Box (%v)", hashBox(box), m.inCounter, m.outCounter, box.Boxtype(), duration)
	//}
	writeChan <- contentconsumer.NewTimedContent(duration, boxes)
	m.durCumul += duration
}

//writeBoxToChannel - Write a box which make the duration into channel
func (m *Mp4Source) writeBoxToChannel(duration time.Duration, box mp4box.Box, writeChan chan<- contentconsumer.TimedContentPtr) {
	m.outCounter++
	//log.Printf("%v %v %v %v Write Box (%v)", hashBox(box), m.inCounter, m.outCounter, box.Boxtype(), duration)
	writeChan <- contentconsumer.NewTimedContent(duration, box)
	m.durCumul += duration
}

//queueBox - But the box into queue for dispatch later
func (m *Mp4Source) queueBox(ctx context.Context, box mp4box.Box) {
	if m.unSentBoxes == nil {
		queue := make([]mp4box.Box, 0)
		m.unSentBoxes = &queue
	}
	*m.unSentBoxes = append(*m.unSentBoxes, box)
	//log.Printf("%v %v %v %v Queue Box", hashBox(box), m.inCounter, m.outCounter, box.Boxtype())
}

//dispatchDefault - dispatch all boxes (except mdat,moof) to channel/queue
func (m *Mp4Source) dispatchDefault(ctx context.Context, box mp4box.Box, writeChan chan<- contentconsumer.TimedContentPtr) error {
	if m.unSentBoxes == nil {
		//Wait till we get the first moof...
		//It will help understand what duration is to be used
		m.queueBox(ctx, box)
		return nil
	}
	//If Moof's are help other boxes are to be queued as well
	if len(*m.unSentBoxes) > 0 {
		m.queueBox(ctx, box)
		return nil
	}
	m.writeBoxToChannel(ZERODuration, box, writeChan)
	return nil
}

//dispatchDefault - dispatch Mdat boxes to channel/queue
func (m *Mp4Source) dispatchMdat(ctx context.Context, box mp4box.Box, writeChan chan<- contentconsumer.TimedContentPtr, fromQueueDispatch bool) error {
	if !fromQueueDispatch {
		if m.unSentBoxes == nil {
			//Wait till we get the first moof...
			//It will help understand what duration is to be used
			m.queueBox(ctx, box)
			//log.Printf("MDAT queue opening")
			return nil
		}
		//If Moof's are help other boxes are to be queued as well
		if len(*m.unSentBoxes) > 0 {
			m.queueBox(ctx, box)
			//log.Printf("MDAT queue as moof pending")
			return nil
		}
	}
	if m.lastMoof.moof != nil {
		m.writeBoxesToChannel(m.lastMoof.duration, []mp4box.Box{m.lastMoof.moof, box}, writeChan)
		m.lastMoof.moof = nil
	} else {
		log.Printf("WARN !! Got MDAT before moof ignoring...!!")
	}
	return nil
}

//dispatchDefault - dispatch Moof boxes to channel/queue
func (m *Mp4Source) dispatchMoof(ctx context.Context, box mp4box.Box, writeChan chan<- contentconsumer.TimedContentPtr) error {
	var err error
	var moofbox *mp4box.MovieFragmentBox
	if box == nil {
		return nil
	}
	moofbox, err = box.GetMovieFragmentBox()
	if err != nil {
		return nil
	}
	if m.lastMoof.moof != nil {
		log.Printf("WARN !! MOOF arrived when last moof is still open!!")
		m.lastMoof.moof = nil
	}
	sequenceNo, bmdt, trackID, timescale := moofbox.Summary()
	if sequenceNo == nil || bmdt == nil || trackID == nil {
		log.Printf("NIL VALUES %v %v %v", sequenceNo, bmdt, trackID)
	}
	_, oldTimeScale := m.recordTimeScale(timescale)
	if oldTimeScale != nil {
		if m.StatzAgg != nil {
			m.StatzAgg.PostEventStats(ctx, &statzagg.EventStats{
				EventClock: time.Now(),
				ID:         m.id,
				Name:       EvtMp4TimeScaleAltered,
			})
		}
		//log.Printf("WARN !! TimeScale Updated!!")
	}
	_, oldTrackID := m.recordTrackID(trackID)
	if oldTrackID != nil {
		if m.StatzAgg != nil {
			m.StatzAgg.PostEventStats(ctx, &statzagg.EventStats{
				EventClock: time.Now(),
				ID:         m.id,
				Name:       EvtMp4TrackIDAltered,
			})
		}
		//log.Printf("WARN !! TrackID Updated!!")
	}
	_, oldBaseMediaDecodeTime := m.recordBaseMediaDecodetime(bmdt)
	_, oldSequenceNo := m.recordSequenceNo(sequenceNo)
	if oldSequenceNo != nil {
		if *m.sequenceNo != *oldSequenceNo+1 {
			if m.StatzAgg != nil {
				values := make([]interface{}, 2)
				values[0] = *oldSequenceNo
				values[0] = *m.sequenceNo
				m.StatzAgg.PostEventStats(ctx, &statzagg.EventStats{
					EventClock: time.Now(),
					ID:         m.id,
					Name:       EvtMp4SequenceMissed,
					Values:     values,
				})
			}
		}
	}
	//If TimeScale & BaseMediaDecodeTime is available
	if m.timeScale != nil && m.baseMediaDecodetime != nil && m.sequenceNo != nil {
		//We now have the data
		if oldBaseMediaDecodeTime == nil || oldSequenceNo == nil {
			//This is the first data we got... we need to wait
			m.queueBox(ctx, box)
		} else {
			var moofDuration time.Duration
			//Handle Sequence WRAP - TBD
			if *m.sequenceNo == *oldSequenceNo+1 {
				//next sequence
				//Handle baseMediaDecodetime WRAP - TBD
				durPts := *m.baseMediaDecodetime - *oldBaseMediaDecodeTime
				durSecs := float64(durPts) / float64(*m.timeScale)
				moofDuration = time.Duration(durSecs*1000000) * time.Microsecond
			}
			m.dispatchAllQueue(ctx, moofDuration, writeChan)
			m.lastMoof.duration = moofDuration
			m.lastMoof.moof = box
		}
	} else {
		if m.StatzAgg != nil {
			//Duration cannot be determined use default
			m.StatzAgg.PostEventStats(ctx, &statzagg.EventStats{
				EventClock: time.Now(),
				ID:         m.id,
				Name:       EvtMp4NoBoxTime,
			})
		}
		m.dispatchAllQueue(ctx, m.DefaultMoofDuration, writeChan)
		m.lastMoof.duration = m.DefaultMoofDuration
		m.lastMoof.moof = box
	}
	return nil
}

//dispatchAllQueue - dispatch all items in queue to channel
func (m *Mp4Source) dispatchAllQueue(ctx context.Context, moofDuration time.Duration, writeChan chan<- contentconsumer.TimedContentPtr) {
	if m.unSentBoxes == nil {
		return
	}
	for len(*m.unSentBoxes) > 0 {
		oldbox := (*m.unSentBoxes)[0]
		(*m.unSentBoxes) = (*m.unSentBoxes)[1:]
		//log.Printf("%v %v %v %v Dequeue Box", hashBox(oldbox), m.inCounter, m.outCounter, oldbox.Boxtype())
		switch oldbox.Boxtype() {
		case "moof":
			//log.Printf("Write OLD MOOF %v", duration)
			m.lastMoof.duration = moofDuration
			m.lastMoof.moof = oldbox
		case "mdat":
			//log.Printf("Write OLD MDAT %v", duration)
			m.dispatchMdat(ctx, oldbox, writeChan, true)
		default:
			//log.Printf("Write OLD NON-MOOF %v", ZERODuration)
			m.writeBoxToChannel(ZERODuration, oldbox, writeChan)
		}
	}
}

//getMp4 - Fetch MP4 from the URL
func (m *Mp4Source) getMp4(ctx context.Context, input dashreader.ChunkURL, writeChan chan<- contentconsumer.TimedContentPtr) (statz *statzagg.HTTPClientStatz) {
	var req *http.Request
	var err error
	statz = new(statzagg.HTTPClientStatz)
	statz.ID = m.id
	statz.URL = input.ChunkURL.String()

	client := &http.Client{
		Timeout: m.Timeout,
	}
	time.Sleep(input.FetchAt.Sub(time.Now()))
	req, err = http.NewRequest(http.MethodGet, input.ChunkURL.String(), nil)
	if err != nil {
		statz.Err = fmt.Errorf("NewRequest Fail: %w", err)
		return
	}
	//Pass cancel context
	req = req.WithContext(ctx)
	// Create go-httpstat powered context and pass it to http.Request
	sCtx := httpstat.WithHTTPStat(req.Context(), &statz.Result)
	req = req.WithContext(sCtx)

	//Start a clock for overall duration compute
	statz.BegClock = time.Now()
	defer func() {
		statz.EndClock = time.Now()
	}()

	res, err := client.Do(req)
	if err != nil {
		statz.Err = fmt.Errorf("client.Do Fail: %w", err)
		return
	}
	defer res.Body.Close()
	err = statz.ReadHTTPHeader(&res.Header)
	if err != nil {
		statz.Err = fmt.Errorf("statz.ReadHTTPHeader Fail: %w", err)
		return
	}
	statz.Status = res.StatusCode
	//Check for error
	switch res.StatusCode {
	case http.StatusOK: //200 OK
		break
	default:
		body, _ := ioutil.ReadAll(res.Body)
		if len(body) > 0 {
			statz.Err = fmt.Errorf("\"%v\"", string(body))
		} else {
			statz.Err = fmt.Errorf("Http:NotOK:%v", res.StatusCode)
		}
		return
	}
	//Decode MP4 Boxes
	decoder := mp4box.NewBoxReader(res.Body)
	i := 0
	for {
		i++
		box, err := decoder.NextBox()
		//log.Printf("New Box %v %v", box.Boxtype(), hashBox(box))
		if err != nil {
			if !errors.Is(err, io.EOF) {
				statz.Err = fmt.Errorf("Decoder.NextBox Fail: %w", err)
			}
			return
		}
		m.inCounter++
		//log.Printf("%v %v %v %v New Box", hashBox(box), m.inCounter, m.outCounter, box.Boxtype())
		statz.Bytes += box.Size()
		m.handleBox(ctx, box)
		switch box.Boxtype() {
		case "moof":
			err = m.dispatchMoof(ctx, box, writeChan)
			if err != nil {
				statz.Err = fmt.Errorf("dispatchMoov Fail: %w", err)
				return
			}
		case "mdat":
			err = m.dispatchMdat(ctx, box, writeChan, false)
			if err != nil {
				statz.Err = fmt.Errorf("dispatchMoov Fail: %w", err)
				return
			}
		default: //rest all including MOOF
			err = m.dispatchDefault(ctx, box, writeChan)
			if err != nil {
				statz.Err = fmt.Errorf("dispatchDefault Fail: %w", err)
				return
			}
		}
		runtime.Gosched()
	}
}

//Helpers

//hashBox - generate a hash for the box
func hashBox(box mp4box.Box) []uint64 {
	bytes := []byte(box.String())
	h := md5.New()
	byteData := h.Sum(bytes)
	n1 := binary.BigEndian.Uint64(byteData[0:8])
	n2 := binary.BigEndian.Uint64(byteData[8:16])
	return []uint64{n1, n2}
}

//checkAndUpdateUint32 - checks and stores update for uint32
func checkAndUpdateUint32(store **uint32, input *uint32) (updated bool, oldValue *uint32) {
	updated = false
	oldValue = nil
	if store == nil {
		panic("cannot have store as nil")
	}
	if input == nil {
		return
	}
	//incoming value exists
	if *store == nil {
		//old value not exits... set new value
		updated = true
		*store = new(uint32)
		**store = *input
		return
	}
	//already value present
	if **store == *input {
		//no update
		return
	}
	//update and old value exists
	oldValue = *store
	updated = true
	*store = new(uint32)
	**store = *input
	return
}

//checkAndUpdateUint64 - checks and stores update for uint64
func checkAndUpdateUint64(store **uint64, input *uint64) (updated bool, oldValue *uint64) {
	updated = false
	oldValue = nil
	if store == nil {
		panic("cannot have store as nil")
	}
	if input == nil {
		return
	}
	//incoming value exists
	if *store == nil {
		//old value not exits... set new value
		updated = true
		*store = new(uint64)
		**store = *input
		return
	}
	//already value present
	if **store == *input {
		//no update
		return
	}
	//update and old value exists
	oldValue = *store
	updated = true
	*store = new(uint64)
	**store = *input
	return
}
