#!/usr/bin/python

import sys
import psycopg2
import time
import datetime
import xml.etree.ElementTree as ET
import copy
import argparse
import csv
import urllib2 as url2
import HTMLRunListParser
import os
import samweb_client
import urllib2 as url2
import json
import sqlite3

            
#.......................................................................
class RunInfo:
  
# Define parameters for a Run

  def __init__(self,run):
    self.RunNumber = run            # Set Run Number
    self.RunType = 0
    self.ConfigID = 0               # Configuration ID Number
    self.SubConfigType = 0
    self.TimeStart = "N/A"          # Start Time of the Run
    self.TimeStop = "N/A"           # Stop Time of the Run

    self.GoodForPhysAna = "Unknown"  # Is Run Good for BNB Analysis
    self.RunConfig = "N/A"          # Run configuration, e.g. 'Physics', 'PMT', etc.

    # BNB Status
    self.BNB = "N/A"                # Did the Run Configuration include BNB triggers?
        
    # NuMI Status
    self.NuMI = "N/A"               # Did the Run Configuration include NuMI triggers?

    # Detector Power
    self.DetPowerOn = "N/A"         # Is Detector Power On? (Asics + Crate Rails)
    self.DetPowerOnFrac = 0         # Fraction of Run w/ Detector Power On
    self.DetPowerOffFrac = 0        # Fraction of Run w/ Detector Power Off
    self.DetPowerUnknownFrac = 0    # Fraction of Run w/ Detector Power Unknown
    self.DetPowerStatusList = []    # List of status changes for Detector Power

    # Global Run/Subsystem Status
    self.ShifterCheck = "N/A"
    self.GoodRunFrac = 0
    self.BadRunFrac = 0
    self.UnknownRunFrac = 0
    self.GoodSubRunBlocks = []   # List of blocks of Good Subruns
    self.GlobalStatus = []       # List of changes to Global Run Status, with timestamps
    self.SubSystStatus = dict()  # Dictionary of Subsystem status lists: key - Subsystem name
                                 #                                       val - SubSystemStatus class 

                                                              
  #.......................................................................
  def InitSubSystStatus(self, subSystList):
    
    # Loop over the list of Subsystems
    for syst in subSystList:
      self.SubSystStatus[syst] = SubSystemStatus()
      for chan in subSystList[syst].chlist: subSystList[syst].chlist[chan].Reset()
        
  #.......................................................................
  def GetRunConfig(self):
    rccur = rcconn.cursor()

    # Setup and execute MainRun DB query
    cmd = "select RunType, ConfigID from MainRun where RunNumber = %i" % self.RunNumber
    if verbosity > 1: print "Executing command:", cmd 
    try: rccur.execute(cmd)
    except:
      rccur.close()
      return -1

    try: rt = rccur.fetchone()
    except:
      rccur.close()
      return -1
    
    if rt == None:
      print "No result"
      rccur.close()
      return -1

    # Store the Run Info
    self.RunType = rt[0]
    self.ConfigID = rt[1]
    

    # Setup and execute MainConfigTable DB query
    cmd = "select ConfigName from MainConfigTable where ConfigID = %i and RunType = %i" % (self.ConfigID, self.RunType)
    if verbosity > 1: print "Executing command:", cmd
    try: rccur.execute(cmd)
    except:
      self.RunConfig = "Unknown"
      rccur.close()
      return 0

    # Get Configuration Name
    try: rt = rccur.fetchone()
    except:
      self.RunConfig = "Unknown"
      rccur.close()
      return 0
    if rt == None:
      self.RunConfig = "Unknown"
      rccur.close()
      return 0
    ConfigName = rt[0]


    # Is it a Physics Run?
    if "PHYSICS" in ConfigName or "Physics" in ConfigName or "physics" in ConfigName:
      self.RunConfig = "Physics"
    elif "BeamTimeStudy" in ConfigName:
      self.RunConfig = "BeamTimeStudy"
    elif "CosmicDisc" in ConfigName:
      self.RunConfig = "CosmicDiscStudy"
    elif "external_trigger" in ConfigName or "ExternalTriggerOnly" in ConfigName or "ExtTriggerOnly" in ConfigName:
      self.RunConfig = "ExternalTrigger"
    elif "laser_trigger" in ConfigName or "LaserTriggerOnly" in ConfigName:
      self.RunConfig = "Laser"
    elif "PaddleOnly" in ConfigName or "PaddleTrigger" in ConfigName:
      self.RunConfig = "Paddle"
    elif "PMTFlasher" in ConfigName or "PMTNoiseTest" in ConfigName or "PMTTrigger" in ConfigName:
      self.RunConfig = "PMT"
    elif "TriggerStudy" in ConfigName:
      self.RunConfig = "Trigger"
    else: self.RunConfig = "Other"


    # Check if Run Config includes BNB/NuMI triggers
    if "BNB" in ConfigName: self.BNB = "Yes"
    else: self.BNB = "No"
    if "NUMI" in ConfigName: self.NuMI = "Yes"
    else: self.NuMI = "No"

    if self.BNB == "Yes" and self.NuMI == "Yes": self.RunConfig += " (BNB,NuMI)"
    elif self.BNB == "Yes": self.RunConfig += " (BNB)"
    elif self.NuMI == "Yes": self.RunConfig += " (NuMI)"

    
    rccur.close()
    return 1
  

  #.......................................................................
  @staticmethod
  def EvalConfig(RunConfig):
    # Search for substring 'Physcis' in RunConfig string
    if "Physics" in RunConfig: return True
    else: return False

      
  #.......................................................................
  def GetRunBoundary(self):
    smcur = smconn.cursor()
    rccur = rcconn.cursor()    

    # First get Run Start Time from uB_DAQStatus_DAQX_runcontrol/current_run.
    # If there is no entry then the run likely crashed with no data taken.
    cmd = "select smpl_time from sample where channel_id = (select channel_id from channel where name = 'uB_DAQStatus_DAQX_runcontrol/current_run') and float_val = %i" % self.RunNumber
    try:
      smcur.execute(cmd)
      rt = smcur.fetchone()
      if rt == None: smTimeStart = None
      else: smTimeStart = rt[0]
    except: smTimeStart = None               

      
    # Get RunNumber, TimeStart, and TimeStop from Main Run Table
    # TimeStart is usually filled, TimeStop only started being filled later
    cmd = "select runnumber, timestart, timestop from MainRun where RunNumber >= %i order by runnumber limit 2" % self.RunNumber
    try:
      rccur.execute(cmd)
      
      rt = rccur.fetchone()
      if rt == None:
        rcTimeStart = None
        rcTimeStop = None
      elif rt[0] == self.RunNumber:
        rcTimeStart = rt[1]
        rcTimeStop = rt[2]

        try:
          rt = rccur.fetchone()
          if rt == None:
            rcNextRun = self.RunNumber
            rcNextTimeStart = None
          else:
            rcNextRun = rt[0]
            rcNextTimeStart = rt[1]
        except:
          rcNextRun = self.RunNumber
          rcNextTimeStart = None
          
      else:
        rcTimeStart = None
        rcTimeStop = None
        rcNextRun = rt[0]
        rcNextTimeStart = rt[1]
        
    except:
      rcTimeStart = None
      rcTimeStop = None
      rcNextRun = self.RunNumber
      rcNextTimeStart = None

    rccur.close()  
    # If Run Start Time is not in either DB, then assume run crashed with no data taken
    if smTimeStart == None and rcTimeStart == None:
      smcur.close()
      return -1

    # Use Run Start Time from uB_DAQStatus_DAQX_runcontrol/current_run,
    # otherwise form MainRun
    if rcTimeStart != None: self.TimeStart = rcTimeStart
    if smTimeStart != None: self.TimeStart = smTimeStart
    if rcTimeStop != None:
      # Use MainRun Run Stop Time if available
      self.TimeStop = rcTimeStop
      smcur.close()
      return 1
    
    # If not, look at next entry in uB_DAQStatus_DAQX_runcontrol/current_run
    # RunNumber = 0.0 indicates a run stop     
    smNextRun = self.RunNumber
    cmd = "select float_val, smpl_time from sample where channel_id = (select channel_id from channel where name = 'uB_DAQStatus_DAQX_runcontrol/current_run') and smpl_time > '%s' order by smpl_time limit 1" % self.TimeStart     
    try:
      smcur.execute(cmd)
      rt = smcur.fetchone()
      if rt == None: smTimeStop = None
      else:
        smTimeStop = rt[1]
        smNextRun = rt[0]
    except: smTimeStop = None
    smcur.close()

    # If the next entry in uB_DAQStatus_DAQX_runcontrol/current_run is 0.0
    # then use that as Run Stop Time
    if smNextRun == 0.0:
      self.TimeStop = smTimeStop
      return 1

    # Otherwise use the lower Run Number between the next entries in
    # uB_DAQStatus_DAQX_runcontrol/current_run and MainRun
    if smNextRun > self.RunNumber and (smNextRun < rcNextRun or rcNextRun == self.RunNumber):
      self.TimeStop = smTimeStop
      return 1
    elif rcNextTimeStart != None:
      self.TimeStop = rcNextTimeStart
      return 1
    else: return 0
                      
          
  #.......................................................................
  def CheckRunCat(self):
    # Check shifter evaluation in Run Catalogue
    cmd = 'http://ubdaq-prod-near2.fnal.gov/RunCat/shift-check.cgi?runs=%i' % self.RunNumber
    try: f = url2.urlopen(cmd)
    except:
      print "Unable to query Run Catalogue for run", self.RunNumber
      return

    run_info = json.load(f)
    try: shift_check = int(run_info['runs'][str(self.RunNumber)]['ok'])
    except:
      self.ShifterCheck = 'Unknown'
      return

    if shift_check == 1: self.ShifterCheck = 'Good'
    elif shift_check == 0: self.ShifterCheck = 'Bad'
    else: self.ShifterCheck = 'Unknown'
      

  #.......................................................................
  def CheckDetPower(self):

    # Evaluate Detector Power (Asics & Crate Rails) status   
    nBad = 0
    nUnknown = 0
    nGood = 0

    SystemStatus = []
    CurrState = dict()
    GlobalRunStatus = -2
    GlobalRunTime = datetime.datetime(2030,1,1)
    
    # Store initial state based on the initial states of each SubSystem                                      
    # Create time-ordered list of status changes of every SubSystem after its initial state  
    StatusByTime = []
    for syst in self.SubSystStatus:
      if syst != "Asics" and syst != "CrateRails": continue
      CurrState[syst] = [self.SubSystStatus[syst].StatusList[0][0],self.SubSystStatus[syst].StatusList[0][1]]
      if CurrState[syst][0] == 0: nBad += 1
      elif CurrState[syst][0] == -1: nUnknown += 1
      else: nGood += 1              
      GlobalRunTime = CurrState[syst][1]
              
      clen = len(self.SubSystStatus[syst].StatusList)
      if clen < 2: continue
      slen = len(StatusByTime)
      for row in range(1,clen):
        if slen == 0:
          StatusByTime.append([syst,self.SubSystStatus[syst].StatusList[row][0],self.SubSystStatus[syst].StatusList[row][1]])
          continue
        ctime = self.SubSystStatus[syst].StatusList[row][1]
        ndx = len(StatusByTime)
        for l in range(0,len(StatusByTime)):
          ltime = StatusByTime[l][2]
          if ctime < ltime:
            ndx = l
            break
        StatusByTime.insert(ndx,[syst,self.SubSystStatus[syst].StatusList[row][0],self.SubSystStatus[syst].StatusList[row][1]])

    if nBad > 0: CurrState["Global"] = [0,GlobalRunTime]
    elif nUnknown > 0: CurrState["Global"] = [-1,GlobalRunTime]
    else: CurrState["Global"] = [1,GlobalRunTime]
    SystemStatus.append(CurrState["Global"])

    # Loop over time-ordred list of Subsystem state changes and update current state of each Subsystem
    for l in range(0,len(StatusByTime)):
        CurrState[StatusByTime[l][0]] = [StatusByTime[l][1],StatusByTime[l][2]]              
        stime = StatusByTime[l][2]
        nBad = 0
        nUnknown = 0
        nGood = 0
        
        # Loop over current state of each Subsystem and evaluate Global Run status
        for syst in CurrState:
            if syst == "Global": continue
            sstat = CurrState[syst][0]
            if sstat == 0: nBad += 1
            elif sstat == -1: nUnknown += 1
            else: nGood += 1
                
        # If Global Run status has changed, store in list along with which Subsystem caused the state change and TimeStamp of the change
        if nBad > 0: GlobalRunStatus = 0
        elif nUnknown > 0: GlobalRunStatus = -1
        else: GlobalRunStatus = 1

        if GlobalRunStatus != CurrState["Global"][0]:
            GlobalRunTime = stime
            CurrState["Global"] = [GlobalRunStatus,GlobalRunTime]
            if GlobalRunTime <= self.TimeStop: SystemStatus.append(CurrState["Global"])

    # Get Good/Bad/Unkown Fractions
    BadFrac = 0
    GoodFrac = 0
    UnknownFrac = 0        
    nStat = len(SystemStatus)
          
    if nStat == 0: UnknownFrac = 1
    elif nStat == 1:
      if SystemStatus[0][0] == 1: GoodFrac = 1
      elif SystemStatus[0][0] == 0: BadFrac = 1
      else: UnknownFrac = 1
    else:
      for l in range(0,nStat):
        if SystemStatus[l][1] > self.TimeStop: continue
        
        if l < nStat - 1:
          if SystemStatus[l+1][1] > self.TimeStop: tdiff = self.TimeStop - SystemStatus[l][1]
          else: tdiff = SystemStatus[l+1][1] - SystemStatus[l][1]
        else: tdiff = self.TimeStop - SystemStatus[l][1]
          
        if SystemStatus[l][0] == 0: BadFrac += tdiff.seconds + tdiff.microseconds/1e6
        elif SystemStatus[l][0] == 1: GoodFrac += tdiff.seconds + tdiff.microseconds/1e6
        else: UnknownFrac += tdiff.seconds + tdiff.microseconds/1e6

      totTime = GoodFrac + BadFrac + UnknownFrac
      GoodFrac /= totTime
      BadFrac /= totTime
      UnknownFrac /= totTime

    self.DetPowerOnFrac = GoodFrac
    self.DetPowerOffFrac = BadFrac
    self.DetPowerUnknownFrac = UnknownFrac
    self.DetPowerStatusList = SystemStatus

    if GoodFrac > 0: self.DetPowerOn = "Yes"
    elif BadFrac >= UnknownFrac: self.DetPowerOn = "No"
    else: self.DetPowerOn = "Unknown"
      
      
  #.......................................................................
  def PrintRunInfo(self):
    print "Run:", self.RunNumber
    print "RunType =", self.RunType
    print "ConfigID =", self.ConfigID
    print "TimeStart =", self.TimeStart
    print "TimeStop =", self.TimeStop
    print "RunConfig =", self.RunConfig
    print "BNB =", self.BNB
    print "NuMI =", self.NuMI


  #.......................................................................
  def ReadSubSystems(self, subSystList):
    
    # Loop over the list of Subsystems
    for syst in subSystList:
      # Query SlowMon data base for each channel
      subSystList[syst].ReadSubSystem(self.TimeStart, self.TimeStop) 
      
      # Evaluate Subsystem status
      self.SubSystStatus[syst] = subSystList[syst].EvalSubSystem(self.TimeStart, self.TimeStop, self.RunNumber)
      if verbosity == 2 or verbosity == 4: self.PrintSubSystem(syst)

  #.......................................................................
  def PrintSubSystem(self, syst):
    print
    print syst
    for entry in range(0,len(self.SubSystStatus[syst].StatusList)):
      print self.SubSystStatus[syst].StatusList[entry][0], self.SubSystStatus[syst].StatusList[entry][1]


  #.......................................................................
  def EvalRun(self):
    nGood = 0
    nBad = 0
    nUnknown = 0    

    # Check if the run is good for analysis
    if "Physics" in self.RunConfig:
      for syst in self.SubSystStatus:
        if syst == "BNB" or syst == "NuMI": continue
        if self.SubSystStatus[syst].On == "Yes": nGood += 1
        elif self.SubSystStatus[syst].On == "No": nBad += 1
        else: nUnknown += 1

      if nBad > 0: self.GoodForPhysAna = "No"
      elif nUnknown > 0: self.GoodForPhysAna = "Unknown"          
      else: self.GoodForPhysAna = "Yes"
    else: self.GoodForPhysAna = "No"
    
    
    # Evaluate Run Status and store time-ordered list of changes
    # If any non-beam Subsystem or both beam SubSystems have status 'Bad', then Run status will be 'Bad'
    # Else if any non-beam Subsystem or both beam SubSystems have status 'Unknown' then Run Status will be 'Unknown'
    # Else Run Status will be 'Good'

    nBad = 0
    nUnknown = 0
    nGood = 0

    CurrState = dict()
    GlobalRunStatus = -2
    GlobalRunTime = datetime.datetime(2030,1,1)
    
    # Store initial Run state based on the initial states of each SubSystem                                      
    # Create time-ordered list of status changes of every SubSystem after its initial state  
    StatusByTime = []
    for syst in self.SubSystStatus:
      if syst == "BNB" or syst == "NuMI": continue
      CurrState[syst] = [self.SubSystStatus[syst].StatusList[0][0],self.SubSystStatus[syst].StatusList[0][1]]
      if CurrState[syst][0] == 0: nBad += 1
      elif CurrState[syst][0] == -1: nUnknown += 1
      else: nGood += 1              
      GlobalRunTime = CurrState[syst][1]
              
      clen = len(self.SubSystStatus[syst].StatusList)
      if clen < 2: continue
      slen = len(StatusByTime)
      for row in range(1,clen):
        if slen == 0:
          StatusByTime.append([syst,self.SubSystStatus[syst].StatusList[row][0],self.SubSystStatus[syst].StatusList[row][1]])
          continue
        ctime = self.SubSystStatus[syst].StatusList[row][1]
        ndx = len(StatusByTime)
        for l in range(0,len(StatusByTime)):
          ltime = StatusByTime[l][2]
          if ctime < ltime:
            ndx = l
            break
        StatusByTime.insert(ndx,[syst,self.SubSystStatus[syst].StatusList[row][0],self.SubSystStatus[syst].StatusList[row][1]])

    if nBad > 0: CurrState["Global"] = [0,GlobalRunTime]
    elif nUnknown > 0: CurrState["Global"] = [-1,GlobalRunTime]
    else: CurrState["Global"] = [1,GlobalRunTime]
    self.GlobalStatus.append(copy.copy(CurrState))
        
          
    if verbosity == 5:
      print "Initial State:", CurrState
      print
      print "nGood =", nGood, "nBad =", nBad, "nUnknown =", nUnknown, "Total = ", nGood+nBad+nUnknown
      print
      print "StatusByTime:", StatusByTime

    # Loop over time-ordred list of Subsystem state changes and update current state of each Subsystem
    for l in range(0,len(StatusByTime)):
        CurrState[StatusByTime[l][0]] = [StatusByTime[l][1],StatusByTime[l][2]]              
        stime = StatusByTime[l][2]
        nBad = 0
        nUnknown = 0
        nGood = 0
        
        if verbosity == 5:
            print 
            print "******************************"
            print "SubSystem State Change:", StatusByTime[l]            
        # Loop over current state of each Subsystem and evaluate Global Run status
        for syst in CurrState:
            if syst == "Global": continue
            sstat = CurrState[syst][0]
            if sstat == 0: nBad += 1
            elif sstat == -1: nUnknown += 1
            else: nGood += 1
                
        # If Global Run status has changed, store in list along with which Subsystem caused the state change and TimeStamp of the change
        if nBad > 0: GlobalRunStatus = 0
        elif nUnknown > 0: GlobalRunStatus = -1
        else: GlobalRunStatus = 1

        if verbosity == 5:
            print
            print "nGood =", nGood, "nBad = ", nBad, "nUnknown = ", nUnknown, "Total =", nGood+nBad+nUnknown

        if GlobalRunStatus != CurrState["Global"][0]:
            GlobalRunTime = stime
            CurrState["Global"] = [GlobalRunStatus,GlobalRunTime]
            if GlobalRunTime <= self.TimeStop: self.GlobalStatus.append(copy.copy(CurrState))

            if GlobalRunStatus != CurrState["Global"][0] and verbosity == 5:
                print
                print "New Global State:", CurrState["Global"]

        elif verbosity == 5:
            print
            print "No Change in Global State:", CurrState["Global"]


    # Get Good/Bad/Unkown Fractions
    BadFrac = 0
    GoodFrac = 0
    UnknownFrac = 0        
    nStat = len(self.GlobalStatus)

    if nStat == 0: UnknownFrac = 1
    elif nStat == 1:
      if self.GlobalStatus[0]["Global"][0] == 1: GoodFrac = 1
      elif self.GlobalStatus[0]["Global"][0] == 0: BadFrac = 1
      else: UnknownFrac = 1
    else:
      for l in range(0,nStat):
        if self.GlobalStatus[l]["Global"][1] > self.TimeStop: continue
        
        if l < nStat - 1:
          if self.GlobalStatus[l+1]["Global"][1] > self.TimeStop: tdiff = self.TimeStop - self.GlobalStatus[l]["Global"][1]
          else: tdiff = self.GlobalStatus[l+1]["Global"][1] - self.GlobalStatus[l]["Global"][1]
        else: tdiff = self.TimeStop - self.GlobalStatus[l]["Global"][1]
          
        if self.GlobalStatus[l]["Global"][0] == 0: BadFrac += tdiff.seconds + tdiff.microseconds/1e6
        elif self.GlobalStatus[l]["Global"][0] == 1: GoodFrac += tdiff.seconds + tdiff.microseconds/1e6
        else: UnknownFrac += tdiff.seconds + tdiff.microseconds/1e6

      totTime = GoodFrac + BadFrac + UnknownFrac
      GoodFrac /= totTime
      BadFrac /= totTime
      UnknownFrac /= totTime

    self.GoodRunFrac = GoodFrac
    self.BadRunFrac = BadFrac
    self.UnknownRunFrac = UnknownFrac
        
    # Re-evaluate Run status using Runs Status By Time
    if "Physics" in self.RunConfig:
      if len(self.GlobalStatus) == 0: self.GoodForPhysAna = "Unknown"
      elif len(self.GlobalStatus) == 1:
        if self.GlobalStatus[0]["Global"][0] == 0: self.GoodForPhysAna = "No"
        elif self.GlobalStatus[0]["Global"][0] == -1: self.GoodForPhysAna = "Unknown"

      if self.RunNumber > 5909:
        if self.ShifterCheck == 'Bad': self.GoodForPhysAna = "No"
        elif self.ShifterCheck == "Unknown" or self.ShifterCheck == 'N/A':
          if self.GoodForPhysAna == "Yes": self.GoodForPhysAna = 'Unknown'

          
  #.......................................................................
  def FindGoodSubRuns(self):
    # Open interface to samweb
    samweb = samweb_client.SAMWebClient(experiment='uboone')
    
    # Loop over list of status changes
    nStatChng = len(self.GlobalStatus)
    for l in range(0,nStatChng):
      # Get starting time
      if self.GlobalStatus[l]["Global"][0] == 1: time1 = self.GlobalStatus[l]["Global"][1].strftime("%Y-%m-%dT%H:%M:%S+00:00")
      else: continue

      # Get ending time
      if l == nStatChng - 1: time2 = (self.TimeStop + datetime.timedelta(seconds=1.0)).strftime("%Y-%m-%dT%H:%M:%S+00:00")
      else: time2 = (self.GlobalStatus[l+1]["Global"][1] + datetime.timedelta(seconds=1.0)).strftime("%Y-%m-%dT%H:%M:%S+00:00")

      # Get list of files from SAM
      qstr = "data_tier=raw and run_number=%i and file_name=%%.ubdaq and start_time>'%s' and end_time<'%s'" % (RunList[r], time1, time2)
      try: flist = sorted(list(samweb.listFiles(qstr)))
      except:
        print "Could not get files from SAM"
        continue

      # Get good subruns for this time block
      nGoodSubRuns = len(flist)
      GoodSubRunList = []
      if nGoodSubRuns > 0:
        GoodSubRunList.append(int(flist[0].split('-')[3].split('.')[0]))
        GoodSubRunList.append(int(flist[nGoodSubRuns-1].split('-')[3].split('.')[0]))

        # Store block of good subruns in list
        self.GoodSubRunBlocks.append(GoodSubRunList)

            
  #.......................................................................
  def PrintRunStatusByTime(self):
    print
    print "|_ Run Number _| |_ Run Config _| |_ BNB Trig _| |_ NuMI Trig _| |_ Global Status _|",
    for syst in self.SubSystStatus: print "|_", syst, "_|",
    print "|_ TimeStamp _|",

    #if "Physics" in self.RunConfig:
    for l in range(0,len(self.GlobalStatus)):
      print
      print "|_", self.RunNumber, "_| |_", self.RunConfig, "_| |_", self.BNB, "_| |_", self.NuMI, "_| Global |_", self.GlobalStatus[l]["Global"][0],
      for syst in self.GlobalStatus[l]:
        if syst == "Global": continue
        print "_|", syst,
        if self.GlobalStatus[l][syst][1] > self.GlobalStatus[l]["Global"][1]: print "|_ -1 _|",
        else: print "|_", self.GlobalStatus[l][syst][0], "_|",
      print "|_", self.GlobalStatus[l]["Global"][1], "_|",
    #else:
      #print
      #print "|_", self.RunNumber, "_| |_", self.RunConfig, "_| |_", self.BNB, "_| |_", self.NuMI, "_| |_  _| |_  _|"
      #for syst in self.SubSystStatus: print "|_  _|",


  #.......................................................................
  def PrintRunStatus(self, subSystList):
    print
    print "Overall Status of Run", self.RunNumber
    print "\tRun Type:", self.RunConfig
    print "\tGood For Physics Analysis:", self.GoodForPhysAna
    print "\tShifter Check:", self.ShifterCheck
    print "\tGood Fraction:", self.GoodRunFrac
    print "\tBad Fraction:", self.BadRunFrac
    print "\tUnknown Fraction:", self.UnknownRunFrac

    print "\nBNB Status:"    
    print "\tGood BNB Fraction:", self.SubSystStatus["BNB"].GoodFrac
    print "\tBad BNB Fraction:", self.SubSystStatus["BNB"].BadFrac
    print "\tUnknown BNB Fraction:", self.SubSystStatus["BNB"].UnknownFrac
    print "\tBNB Intensity: Avg =", subSystList['BNB'].chlist['uB_BeamData_BEAM_BNB_TOR860/protons'].avg, "E12", " Max =", subSystList['BNB'].chlist['uB_BeamData_BEAM_BNB_TOR860/protons'].max, "E12", " Min =", subSystList['BNB'].chlist['uB_BeamData_BEAM_BNB_TOR860/protons'].min, "E12"
    print "\tBNB Horn Current: Avg =", subSystList['BNB'].chlist['uB_BeamData_BEAM_BNB_THCURR/current'].avg, "kA", " Max =", subSystList['BNB'].chlist['uB_BeamData_BEAM_BNB_THCURR/current'].max, "kA", " Min =", subSystList['BNB'].chlist['uB_BeamData_BEAM_BNB_THCURR/current'].min, "kA"
    print "\tBNB Rate: Avg =", subSystList['BNB'].chlist['uB_BeamData_BEAM_BNB_MBPRTE/act_rate'].avg, "Hz", " Max =", subSystList['BNB'].chlist['uB_BeamData_BEAM_BNB_MBPRTE/act_rate'].max, "Hz", " Min =", subSystList['BNB'].chlist['uB_BeamData_BEAM_BNB_MBPRTE/act_rate'].min, "Hz"
    print "\tBNB Trigger Rate: Avg =", subSystList['BNB'].chlist['uB_DAQStatus_DAQX_evb/Builder_TrigRate_BNB'].avg, "Hz", " Max =", subSystList['BNB'].chlist['uB_DAQStatus_DAQX_evb/Builder_TrigRate_BNB'].max, "Hz", " Min =", subSystList['BNB'].chlist['uB_DAQStatus_DAQX_evb/Builder_TrigRate_BNB'].min, "Hz"

    print "\nNuMI Status:"    
    print "\tGood NuMI Fraction:", self.SubSystStatus["NuMI"].GoodFrac
    print "\tBad NuMI Fraction:", self.SubSystStatus["NuMI"].BadFrac
    print "\tUnknown NuMI Fraction:", self.SubSystStatus["NuMI"].UnknownFrac
    print "\tNuMI Intensity: Avg =", subSystList['NuMI'].chlist['uB_BeamData_BEAM_NuMI_TORTGT/protons'].avg, "E12", " Max =", subSystList['NuMI'].chlist['uB_BeamData_BEAM_NuMI_TORTGT/protons'].max, "E12", " Min =", subSystList['NuMI'].chlist['uB_BeamData_BEAM_NuMI_TORTGT/protons'].min, "E12"
    print "\tNuMI LINA Current: Avg =", subSystList['NuMI'].chlist['uB_BeamData_BEAM_NuMI_NSLINA/current'].avg, "kA", " Max =", subSystList['NuMI'].chlist['uB_BeamData_BEAM_NuMI_NSLINA/current'].max, "kA", " Min =", subSystList['NuMI'].chlist['uB_BeamData_BEAM_NuMI_NSLINA/current'].min, "kA"
    print "\tNuMI LINB Current: Avg =", subSystList['NuMI'].chlist['uB_BeamData_BEAM_NuMI_NSLINB/current'].avg, "kA", " Max =", subSystList['NuMI'].chlist['uB_BeamData_BEAM_NuMI_NSLINB/current'].max, "kA", " Min =", subSystList['NuMI'].chlist['uB_BeamData_BEAM_NuMI_NSLINB/current'].min, "kA"
    print "\tNuMI LINC Current: Avg =", subSystList['NuMI'].chlist['uB_BeamData_BEAM_NuMI_NSLINC/current'].avg, "kA", " Max =", subSystList['NuMI'].chlist['uB_BeamData_BEAM_NuMI_NSLINC/current'].max, "kA", " Min =", subSystList['NuMI'].chlist['uB_BeamData_BEAM_NuMI_NSLINC/current'].min, "kA"
    print "\tNuMI LIND Current: Avg =", subSystList['NuMI'].chlist['uB_BeamData_BEAM_NuMI_NSLIND/current'].avg, "kA", " Max =", subSystList['NuMI'].chlist['uB_BeamData_BEAM_NuMI_NSLIND/current'].max, "kA", " Min =", subSystList['NuMI'].chlist['uB_BeamData_BEAM_NuMI_NSLIND/current'].min, "kA"
    print "\tNuMI Trigger Rate: Avg =", subSystList['NuMI'].chlist['uB_DAQStatus_DAQX_evb/Builder_TrigRate_NuMI'].avg, "Hz", " Max =", subSystList['NuMI'].chlist['uB_DAQStatus_DAQX_evb/Builder_TrigRate_NuMI'].max, "Hz", " Min =", subSystList['NuMI'].chlist['uB_DAQStatus_DAQX_evb/Builder_TrigRate_NuMI'].min, "Hz"

    print "\nDrift Status:"    
    print "\tDrift HV Global On:", self.SubSystStatus["TPCDrift"].On
    print "\tDrift HV: Avg =", subSystList['TPCDrift'].chlist['uB_TPCDrift_HV01_1_0/voltage'].avg/1000, "kV", " Max =", subSystList['TPCDrift'].chlist['uB_TPCDrift_HV01_1_0/voltage'].max/1000, "kV", " Min =", subSystList['TPCDrift'].chlist['uB_TPCDrift_HV01_1_0/voltage'].min/1000, "kV"
    print "\tDrift On Fraction:", self.SubSystStatus["TPCDrift"].GoodFrac
    print "\tDrift Off Fraction:", self.SubSystStatus["TPCDrift"].BadFrac
    print "\tDrift Unknown Fraction:", self.SubSystStatus["TPCDrift"].UnknownFrac

    print "\nPMT Status:"    
    print "\tPMT HV Global On:", self.SubSystStatus["PMTHV"].On
    print "\tPMT HV On Fraction:", self.SubSystStatus["PMTHV"].GoodFrac
    print "\tPMT HV Off Fraction:", self.SubSystStatus["PMTHV"].BadFrac
    print "\tPMT HV Unknown Fraction:", self.SubSystStatus["PMTHV"].UnknownFrac

    print "\nWire Bias Status:"    
    print "\tWire Bias Global On:", self.SubSystStatus["TPCWB"].On
    print "\tWire Bias On Fraction:", self.SubSystStatus["TPCWB"].GoodFrac
    print "\tWire Bias Off Fraction:", self.SubSystStatus["TPCWB"].BadFrac
    print "\tWire Bias Unknown Fraction:", self.SubSystStatus["TPCWB"].UnknownFrac

    print "\nDetector Power Status:"    
    print "\tDetector Power Global On:", self.DetPowerOn
    print "\tDetector Power On Fraction:", self.DetPowerOnFrac
    print "\tDetector Power Off Fraction:", self.DetPowerOffFrac
    print "\tDetector Power Unknown Fraction:", self.DetPowerUnknownFrac

    print "\nDAQ Status:"    
    print "\tDAQ Status:", self.SubSystStatus["DAQ"].On
    print "\tDAQ Good Fraction:", self.SubSystStatus["DAQ"].GoodFrac
    print "\tDAQ Bad Fraction:", self.SubSystStatus["DAQ"].BadFrac
    print "\tDAQ Unknown Fraction:", self.SubSystStatus["DAQ"].UnknownFrac

    print "\nPurity Monitor Status:"    
    print "\tPurity Monitor Status:", self.SubSystStatus["PurityMon"].On
    print "\tLifetime:", 1000*subSystList['PurityMon'].chlist['uB_ArPurity_PM02_1/LIFETIME'].avg

    print "\nGood SubRun Blocks:"
    for bl in range(0,len(self.GoodSubRunBlocks)):
      if len(self.GoodSubRunBlocks[bl]) > 1: print self.GoodSubRunBlocks[bl][0], "-", self.GoodSubRunBlocks[bl][len(self.GoodSubRunBlocks[bl]) - 1]
      elif len(self.GoodSubRunBlocks[bl]) == 1: print self.GoodSubRunBlocks[bl][0]
    
        
  #.......................................................................
  def WriteToDB(self, subSystList):
    #print 
    #print "Writing to RunStatus DB Table..."

    cur = grconn.cursor()

    # Search for entries with this run number
    t = (self.RunNumber,)
    cur.execute('select max(version) from RunStatus where run_number = ?', t)
    rt = cur.fetchone()
    if rt[0] == None:
      # If no previous entries for this run number, get max(version) for all entries
      cur.execute('select max(version) from RunStatus')
      rt2 = cur.fetchone()
      if rt2[0] == None: version = 0
      else: version = int(rt2[0])
    else: version = int(rt[0]) + 1

        
    # Setup data to insert into table    
    cols = ["run_number", "run_config", "phys_status", "good_frac", "bad_frac", "unknown_frac", "shifter_check",
            "pmt_status", "pmt_good_frac", "pmt_bad_frac", "pmt_unknown_frac",
            "wb_status", "wb_good_frac", "wb_bad_frac", "wb_unknown_frac",
            "drift_status", "avg_drift", "max_drift", "min_drift",
            "detpower_status", "detpower_good_frac", "detpower_bad_frac", "detpower_unknown_frac",
            "lifetime_status", "lifetime", "daq_status",
            "bnb_good_frac", "bnb_bad_frac", "bnb_unknown_frac", "avg_bnb_intensity", "max_bnb_intensity", "min_bnb_intensity", "avg_current", "max_current", "min_current",
            "avg_bnb_rate", "max_bnb_rate", "min_bnb_rate", "avg_bnb_trigrate", "max_bnb_trigrate", "min_bnb_trigrate",
            "numi_good_frac", "numi_bad_frac", "numi_unknown_frac", "avg_numi_intensity", "max_numi_intensity", "min_numi_intensity", "avg_lina_current", "max_lina_current", "min_lina_current",
            "avg_linb_current", "max_linb_current", "min_linb_current", "avg_linc_current", "max_linc_current", "min_linc_current",
            "avg_lind_current", "max_lind_current", "min_lind_current", "avg_numi_trigrate", "max_numi_trigrate", "min_numi_trigrate",
            "start_time", "end_time", "version"]
    vals = []
    vals.append(self.RunNumber)
    vals.append(self.RunConfig)
    vals.append(self.GoodForPhysAna)
    vals.append(self.GoodRunFrac)
    vals.append(self.BadRunFrac)
    vals.append(self.UnknownRunFrac)
    vals.append(self.ShifterCheck)

    vals.append(self.SubSystStatus["PMTHV"].On)
    vals.append(self.SubSystStatus["PMTHV"].GoodFrac)
    vals.append(self.SubSystStatus["PMTHV"].BadFrac)
    vals.append(self.SubSystStatus["PMTHV"].UnknownFrac)
    
    vals.append(self.SubSystStatus["TPCWB"].On)
    vals.append(self.SubSystStatus["TPCWB"].GoodFrac)
    vals.append(self.SubSystStatus["TPCWB"].BadFrac)
    vals.append(self.SubSystStatus["TPCWB"].UnknownFrac)

    vals.append(self.SubSystStatus["TPCDrift"].On)
    vals.append(subSystList['TPCDrift'].chlist['uB_TPCDrift_HV01_1_0/voltage'].avg/1000)
    vals.append(subSystList['TPCDrift'].chlist['uB_TPCDrift_HV01_1_0/voltage'].max/1000)
    vals.append(subSystList['TPCDrift'].chlist['uB_TPCDrift_HV01_1_0/voltage'].min/1000)

    vals.append(self.DetPowerOn)
    vals.append(self.DetPowerOnFrac)
    vals.append(self.DetPowerOffFrac)
    vals.append(self.DetPowerUnknownFrac)

    vals.append(self.SubSystStatus["PurityMon"].On)
    vals.append(1000*subSystList['PurityMon'].chlist['uB_ArPurity_PM02_1/LIFETIME'].avg)
    vals.append(self.SubSystStatus["DAQ"].On)

    vals.append(self.SubSystStatus["BNB"].GoodFrac)
    vals.append(self.SubSystStatus["BNB"].BadFrac)
    vals.append(self.SubSystStatus["BNB"].UnknownFrac)
    vals.append(subSystList['BNB'].chlist['uB_BeamData_BEAM_BNB_TOR860/protons'].avg)
    vals.append(subSystList['BNB'].chlist['uB_BeamData_BEAM_BNB_TOR860/protons'].max)
    vals.append(subSystList['BNB'].chlist['uB_BeamData_BEAM_BNB_TOR860/protons'].min)
    vals.append(subSystList['BNB'].chlist['uB_BeamData_BEAM_BNB_THCURR/current'].avg)
    vals.append(subSystList['BNB'].chlist['uB_BeamData_BEAM_BNB_THCURR/current'].max)
    vals.append(subSystList['BNB'].chlist['uB_BeamData_BEAM_BNB_THCURR/current'].min)
    vals.append(subSystList['BNB'].chlist['uB_BeamData_BEAM_BNB_MBPRTE/act_rate'].avg)
    vals.append(subSystList['BNB'].chlist['uB_BeamData_BEAM_BNB_MBPRTE/act_rate'].max)
    vals.append(subSystList['BNB'].chlist['uB_BeamData_BEAM_BNB_MBPRTE/act_rate'].min)
    vals.append(subSystList['BNB'].chlist['uB_DAQStatus_DAQX_evb/Builder_TrigRate_BNB'].avg)
    vals.append(subSystList['BNB'].chlist['uB_DAQStatus_DAQX_evb/Builder_TrigRate_BNB'].max)
    vals.append(subSystList['BNB'].chlist['uB_DAQStatus_DAQX_evb/Builder_TrigRate_BNB'].min)

    vals.append(self.SubSystStatus["NuMI"].GoodFrac)
    vals.append(self.SubSystStatus["NuMI"].BadFrac)
    vals.append(self.SubSystStatus["NuMI"].UnknownFrac)
    vals.append(subSystList['NuMI'].chlist['uB_BeamData_BEAM_NuMI_TORTGT/protons'].avg)
    vals.append(subSystList['NuMI'].chlist['uB_BeamData_BEAM_NuMI_TORTGT/protons'].max)
    vals.append(subSystList['NuMI'].chlist['uB_BeamData_BEAM_NuMI_TORTGT/protons'].min)
    vals.append(subSystList['NuMI'].chlist['uB_BeamData_BEAM_NuMI_NSLINA/current'].avg)
    vals.append(subSystList['NuMI'].chlist['uB_BeamData_BEAM_NuMI_NSLINA/current'].max)
    vals.append(subSystList['NuMI'].chlist['uB_BeamData_BEAM_NuMI_NSLINA/current'].min)
    vals.append(subSystList['NuMI'].chlist['uB_BeamData_BEAM_NuMI_NSLINB/current'].avg)
    vals.append(subSystList['NuMI'].chlist['uB_BeamData_BEAM_NuMI_NSLINB/current'].max)
    vals.append(subSystList['NuMI'].chlist['uB_BeamData_BEAM_NuMI_NSLINB/current'].min)
    vals.append(subSystList['NuMI'].chlist['uB_BeamData_BEAM_NuMI_NSLINC/current'].avg)
    vals.append(subSystList['NuMI'].chlist['uB_BeamData_BEAM_NuMI_NSLINC/current'].max)
    vals.append(subSystList['NuMI'].chlist['uB_BeamData_BEAM_NuMI_NSLINC/current'].min)
    vals.append(subSystList['NuMI'].chlist['uB_BeamData_BEAM_NuMI_NSLIND/current'].avg)
    vals.append(subSystList['NuMI'].chlist['uB_BeamData_BEAM_NuMI_NSLIND/current'].max)
    vals.append(subSystList['NuMI'].chlist['uB_BeamData_BEAM_NuMI_NSLIND/current'].min)
    vals.append(subSystList['NuMI'].chlist['uB_DAQStatus_DAQX_evb/Builder_TrigRate_NuMI'].avg)
    vals.append(subSystList['NuMI'].chlist['uB_DAQStatus_DAQX_evb/Builder_TrigRate_NuMI'].max)
    vals.append(subSystList['NuMI'].chlist['uB_DAQStatus_DAQX_evb/Builder_TrigRate_NuMI'].min)

    vals.append(str(self.TimeStart))
    vals.append(str(self.TimeStop))
    vals.append(version)
      
    instr = "insert into RunStatus ("
    for c in range(0,len(cols) - 1): instr += cols[c] + ","
    instr += cols[len(cols) - 1] + ") values ("
    
    for v in range (0,len(vals) - 1):
      if type(vals[v]) is str: instr += "'" + vals[v] + "',"
      else: instr += str(vals[v]) + ","
    instr += str(vals[len(vals) - 1]) + ")"
    
    #print instr
    try: cur.execute(instr)
    except: print "Unable to write to Good Runs DB Table"

    
    #print 
    #print "Writing to GoodSubRuns DB Table..."

    cols = ["run_number", "min_subrun", "max_subrun", "version"]
    vals = [self.RunNumber, 0, 0, version]
    for sr in range(0,len(self.GoodSubRunBlocks)):
      vals[1] = self.GoodSubRunBlocks[sr][0]
      vals[2] = self.GoodSubRunBlocks[sr][1]
      instr = "insert into GoodSubRuns ("
      for c in range(0,len(cols) - 1): instr += cols[c] + ","
      instr += cols[len(cols) - 1] + ") values ("
      
      for v in range (0,len(vals) - 1): instr += str(vals[v]) + ","
      instr += str(vals[len(vals) - 1]) + ")"
      #print instr
      try: cur.execute(instr)
      except: print "Unable to write to Good Subruns DB Table"

    # Commit DB changes    
    grconn.commit()

        
  #.......................................................................
  def WriteToWeb(self, subSystList):    
    if os.path.exists('/home/grafnj/GoodRuns/Web/RunSets/'): webdir = '/home/grafnj/GoodRuns/Web/RunSets/'
    elif os.path.exists('/uboone/app/users/grafnj/GoodRuns/Web/RunSets/'): webdir = '/uboone/app/users/grafnj/GoodRuns/Web/RunSets/'
    else: webdir = 'Web/RunSets/'
    
    RunStr = str(self.RunNumber) 
    if len(RunStr) < 3: fstr = webdir + "GoodRunList_00000-00099.html"
    elif len(RunStr) == 3: fstr = webdir + "GoodRunList_00%s00-00%s99.html" % (RunStr[0], RunStr[0])
    elif len(RunStr) == 4: fstr = webdir + "GoodRunList_0%s00-0%s99.html" % (RunStr[0:2], RunStr[0:2])
    elif len(RunStr) == 5: fstr = webdir + "/GoodRunList_%s00-%s99.html" % (RunStr[0:3], RunStr[0:3])
    else: return

    f = open(fstr, 'r')
    contents = f.readlines()
    f.close()

    # Setup Table Header
    Header = ["Run","Config","Good For Analysis","Shifter Check","Run Status","Drift HV","PMT HV","Wire Bias","Det Power","Lifetime","Good DAQ Status",
              "BNB Intensity","BNB Horn Current","BNB Rate","BNB Trigger Rate",
              "NuMI Intensity","NuMI LINA Current","NuMI LINB Current","NuMI LINC Current","NuMI LIND Current","NuMI Trigger Rate",
              "Start","End","DQM Plots"]

    # Define Data to write for this Run
    runFracStr = "Good: %.2f <br> Bad: %.2f <br> n/a: %.2f" % (self.GoodRunFrac, self.BadRunFrac, self.UnknownRunFrac)
    
    bnbIntStr = "Avg=%.2fE12 <br> Max=%.2fE12 <br> Min=%.2fE12" % (subSystList['BNB'].chlist['uB_BeamData_BEAM_BNB_TOR860/protons'].avg, subSystList['BNB'].chlist['uB_BeamData_BEAM_BNB_TOR860/protons'].max, subSystList['BNB'].chlist['uB_BeamData_BEAM_BNB_TOR860/protons'].min)
    bnbHCStr = "Avg=%.1f kA <br> Max=%.1f kA <br> Min=%.1f kA" % (subSystList['BNB'].chlist['uB_BeamData_BEAM_BNB_THCURR/current'].avg, subSystList['BNB'].chlist['uB_BeamData_BEAM_BNB_THCURR/current'].max, subSystList['BNB'].chlist['uB_BeamData_BEAM_BNB_THCURR/current'].min)
    bnbRateStr = "Avg=%.2f Hz <br> Max=%.2f Hz <br> Min=%.2f Hz" % (subSystList['BNB'].chlist['uB_BeamData_BEAM_BNB_MBPRTE/act_rate'].avg, subSystList['BNB'].chlist['uB_BeamData_BEAM_BNB_MBPRTE/act_rate'].max, subSystList['BNB'].chlist['uB_BeamData_BEAM_BNB_MBPRTE/act_rate'].min)
    bnbTrigStr = "Avg=%.2f Hz <br> Max=%.2f Hz <br> Min=%.2f Hz" % (subSystList['BNB'].chlist['uB_DAQStatus_DAQX_evb/Builder_TrigRate_BNB'].avg, subSystList['BNB'].chlist['uB_DAQStatus_DAQX_evb/Builder_TrigRate_BNB'].max, subSystList['BNB'].chlist['uB_DAQStatus_DAQX_evb/Builder_TrigRate_BNB'].min)
    
    numiFracStr = "Good: %.2f <br> Bad: %.2f <br> n/a: %.2f" % (self.SubSystStatus["NuMI"].GoodFrac, self.SubSystStatus["NuMI"].BadFrac, self.SubSystStatus["NuMI"].UnknownFrac)
    numiIntStr = "Avg=%.1fE12 <br> Max=%.1fE12 <br> Min=%.1fE12" % (subSystList['NuMI'].chlist['uB_BeamData_BEAM_NuMI_TORTGT/protons'].avg, subSystList['NuMI'].chlist['uB_BeamData_BEAM_NuMI_TORTGT/protons'].max, subSystList['NuMI'].chlist['uB_BeamData_BEAM_NuMI_TORTGT/protons'].min)
    numiLinAStr = "Avg=%.1f kA <br> Max=%.1f kA <br> Min=%.1f kA" % (subSystList['NuMI'].chlist['uB_BeamData_BEAM_NuMI_NSLINA/current'].avg, subSystList['NuMI'].chlist['uB_BeamData_BEAM_NuMI_NSLINA/current'].max, subSystList['NuMI'].chlist['uB_BeamData_BEAM_NuMI_NSLINA/current'].min)
    numiLinBStr = "Avg=%.1f kA <br> Max=%.1f kA <br> Min=%.1f kA" % (subSystList['NuMI'].chlist['uB_BeamData_BEAM_NuMI_NSLINB/current'].avg, subSystList['NuMI'].chlist['uB_BeamData_BEAM_NuMI_NSLINB/current'].max, subSystList['NuMI'].chlist['uB_BeamData_BEAM_NuMI_NSLINB/current'].min)
    numiLinCStr = "Avg=%.1f kA <br> Max=%.1f kA <br> Min=%.1f kA" % (subSystList['NuMI'].chlist['uB_BeamData_BEAM_NuMI_NSLINC/current'].avg, subSystList['NuMI'].chlist['uB_BeamData_BEAM_NuMI_NSLINC/current'].max, subSystList['NuMI'].chlist['uB_BeamData_BEAM_NuMI_NSLINC/current'].min)
    numiLinDStr = "Avg=%.1f kA <br> Max=%.1f kA <br> Min=%.1f kA" % (subSystList['NuMI'].chlist['uB_BeamData_BEAM_NuMI_NSLIND/current'].avg, subSystList['NuMI'].chlist['uB_BeamData_BEAM_NuMI_NSLIND/current'].max, subSystList['NuMI'].chlist['uB_BeamData_BEAM_NuMI_NSLIND/current'].min)
    numiTrigStr = "Avg=%.2f Hz <br> Max=%.2f Hz <br> Min=%.2f Hz" % (subSystList['NuMI'].chlist['uB_DAQStatus_DAQX_evb/Builder_TrigRate_NuMI'].avg, subSystList['NuMI'].chlist['uB_DAQStatus_DAQX_evb/Builder_TrigRate_NuMI'].max, subSystList['NuMI'].chlist['uB_DAQStatus_DAQX_evb/Builder_TrigRate_NuMI'].min)
    
    pmtFracStr = "On: %.2f <br> Off: %.2f <br> n/a: %.2f" % (self.SubSystStatus["PMTHV"].GoodFrac, self.SubSystStatus["PMTHV"].BadFrac, self.SubSystStatus["PMTHV"].UnknownFrac)
    wbFracStr = "On: %.2f <br> Off: %.2f <br> n/a: %.2f" % (self.SubSystStatus["TPCWB"].GoodFrac, self.SubSystStatus["TPCWB"].BadFrac, self.SubSystStatus["TPCWB"].UnknownFrac)
    driftStr = "Avg=%.2f kV <br> Max=%.2f kV <br> Min=%.2f kV" % (subSystList['TPCDrift'].chlist['uB_TPCDrift_HV01_1_0/voltage'].avg/1000, subSystList['TPCDrift'].chlist['uB_TPCDrift_HV01_1_0/voltage'].max/1000, subSystList['TPCDrift'].chlist['uB_TPCDrift_HV01_1_0/voltage'].min/1000)
    dpFracStr = "On: %.2f <br> Off: %.2f <br> n/a: %.2f" % (self.DetPowerOnFrac, self.DetPowerOffFrac, self.DetPowerUnknownFrac)

    lifeStr = "%.2f ms" % (1000*subSystList['PurityMon'].chlist['uB_ArPurity_PM02_1/LIFETIME'].avg)
    DAQStatusStr = self.SubSystStatus["DAQ"].On
    dqmStr = "N/A"

    try:
      RunData = [RunStr, self.RunConfig, self.GoodForPhysAna, self.ShifterCheck, runFracStr, driftStr, pmtFracStr, wbFracStr, dpFracStr, lifeStr, DAQStatusStr,
                  bnbIntStr, bnbHCStr, bnbRateStr, bnbTrigStr,
                  numiIntStr, numiLinAStr, numiLinBStr, numiLinCStr, numiLinDStr, numiTrigStr, 
                  self.TimeStart.strftime('%Y-%m-%d %H:%M:%S'), self.TimeStop.strftime('%Y-%m-%d %H:%M:%S'), dqmStr]
    except:
      RunData = [RunStr, self.RunConfig, self.GoodForPhysAna, self.ShifterCheck, runFracStr, driftStr, pmtFracStr, wbFracStr, dpFracStr, lifeStr, DAQStatusStr,
                  bnbIntStr, bnbHCStr, bnbRateStr, bnbTrigStr,
                  numiIntStr, numiLinAStr, numiLinBStr, numiLinCStr, numiLinDStr, numiTrigStr, 
                  self.TimeStart, self.TimeStop, dqmStr]

        
    # instantiate the parser and feed it some HTML                                                                     
    parser = HTMLRunListParser.HTMLRunListParser()

    toReplace = -1
    replaceHeader = 0
    lineNo = 0
    dline = 0
    hline = 0
    setHeaderInsertLine = 0
    Offset = 0
    tableInsertLine = 0
    for line in contents:
        parser.feed(line)
        if parser.Data == "Run": hline = 1
        elif hline > 0 and hline < len(Header) and parser.nTableRows == 0:
            if parser.Data != Header[hline]:
                contents[lineNo] = "    <th>%s</th>\n" % Header[hline]
                replaceHeader = 1
            hline += 1

        if parser.nTableRows > 0 and setHeaderInsertLine == 0:
          setHeaderInsertLine = 1
          if hline < len(Header):
            HeaderInsertLine = parser.getpos()[0] - 2
            Offset = len(Header) - hline          
        
        if parser.RunNo > self.RunNumber:
            WriteLine = parser.getpos()[0] - 3 + Offset
            break
        elif parser.EndTable:
            WriteLine = parser.getpos()[0] - 2 + Offset
            break
        elif parser.RunNo == self.RunNumber:
            if dline == 0:
                toReplace = 0
                prevnTableRows = parser.nTableRows
            elif dline > 0 and parser.Data != RunData[dline]:
                if prevnTableRows == parser.nTableRows:
                    contents[lineNo] = "    <td>%s</td>\n" % RunData[dline]
                    toReplace = 1
                elif dline < len(RunData):
                    tableInsertLine = parser.getpos()[0] - 2 + Offset
                    break
            prevnTableRows = parser.nTableRows
            dline += 1
            if dline == len(RunData): break
        lineNo += 1

    if Offset > 0:
        for l in range(hline,len(Header)):
            instr = "    <th>%s</th>\n" % Header[l]
            contents.insert(HeaderInsertLine,instr)
            HeaderInsertLine += 1

    if tableInsertLine > 0:
      for l in range(dline,len(RunData)):
        instr = "    <td>%s</td>\n" % RunData[l]
        contents.insert(tableInsertLine,instr)   
            
    if toReplace == -1:
      contents.insert(WriteLine,"  </tr>\n\n");
      for l in range(0,len(RunData)):
        instr = "    <td>%s</td>\n" % RunData[len(RunData)-1-l]
        contents.insert(WriteLine,instr)
      contents.insert(WriteLine,"  <tr>\n");

    if toReplace != 0 or replaceHeader > 0 or Offset > 0 or tableInsertLine > 0:
      f = open(fstr, 'w')
      f.writelines(contents)
      f.close()


  #.......................................................................
  def WriteToTuple(self, subSystList, fname):
    # Write tuple of Run Info

    # Good for BNB Analysis
    if self.GoodForPhysAna == "Yes": gana = 1
    elif self.GoodForPhysAna == "No": gana = 0
    else: gana = -1

    # Configured for Physics
    if 'Physics' in self.RunConfig: phys = 1
    else: phys = 0

    # TPC Drift On/Off Status
    if self.SubSystStatus["TPCDrift"].On == "Yes": drifton = 1
    elif self.SubSystStatus["TPCDrift"].On == "No": drifton = 0
    else: drifton = -1

    # Run Length
    runsec = (self.TimeStop - self.TimeStart).seconds
    
    outstr = "%i %i %i %f %f %f %i %i %i %f %f %f %i %i %i %i %f %f %f %f %f %f %i %i %i %f %f %f %i %f %f %f %i %i %i %f %f %f %i %i %i %f %f %f %i %i %i %f %f %f %f %i %f\n" % (self.RunNumber, gana, phys, self.SubSystStatus["PMTHV"].GoodFrac, self.SubSystStatus["PMTHV"].BadFrac, self.SubSystStatus["PMTHV"].UnknownFrac, len(self.SubSystStatus["PMTHV"].StatusList), self.SubSystStatus["PMTHV"].nBadChanges, self.SubSystStatus["PMTHV"].nUnknownChanges, self.SubSystStatus["TPCWB"].GoodFrac, self.SubSystStatus["TPCWB"].BadFrac, self.SubSystStatus["TPCWB"].UnknownFrac, len(self.SubSystStatus["TPCWB"].StatusList), self.SubSystStatus["TPCWB"].nBadChanges, self.SubSystStatus["TPCWB"].nUnknownChanges, drifton, subSystList['TPCDrift'].chlist['uB_TPCDrift_HV01_1_0/voltage'].avg/1000, subSystList['TPCDrift'].chlist['uB_TPCDrift_HV01_1_0/voltage'].max/1000, subSystList['TPCDrift'].chlist['uB_TPCDrift_HV01_1_0/voltage'].min/1000, self.SubSystStatus["TPCDrift"].GoodFrac, self.SubSystStatus["TPCDrift"].BadFrac, self.SubSystStatus["TPCDrift"].UnknownFrac, len(self.SubSystStatus["TPCDrift"].StatusList), self.SubSystStatus["TPCDrift"].nBadChanges, self.SubSystStatus["TPCDrift"].nUnknownChanges, self.DetPowerOnFrac, self.DetPowerOffFrac, self.DetPowerUnknownFrac, len(self.DetPowerStatusList), self.SubSystStatus["Asics"].GoodFrac, self.SubSystStatus["Asics"].BadFrac, self.SubSystStatus["Asics"].UnknownFrac, len(self.SubSystStatus["Asics"].StatusList), self.SubSystStatus["Asics"].nBadChanges, self.SubSystStatus["Asics"].nUnknownChanges, self.SubSystStatus["CrateRails"].GoodFrac, self.SubSystStatus["CrateRails"].BadFrac, self.SubSystStatus["CrateRails"].UnknownFrac, len(self.SubSystStatus["CrateRails"].StatusList), self.SubSystStatus["CrateRails"].nBadChanges, self.SubSystStatus["CrateRails"].nUnknownChanges, self.SubSystStatus["DAQ"].GoodFrac, self.SubSystStatus["DAQ"].BadFrac, self.SubSystStatus["DAQ"].UnknownFrac, len(self.SubSystStatus["DAQ"].StatusList), self.SubSystStatus["DAQ"].nBadChanges, self.SubSystStatus["DAQ"].nUnknownChanges, 1000*subSystList['PurityMon'].chlist['uB_ArPurity_PM02_1/LIFETIME'].avg, self.GoodRunFrac, self.BadRunFrac, self.UnknownRunFrac, len(self.GlobalStatus), runsec)
    
    with open(fname,'ab') as f: f.write(outstr)

      
#.......................................................................
class SubSystemStatus:

  # Class for storing SubSystem Status
  def __init__(self):
    self.StatusList = []      # List of SubSystem status changes, with timestamps  
    self.GoodFrac = 0         # Fraction of time SubSystem was Good
    self.BadFrac = 0          # Fraction of time SubSystem was Bad
    self.UnknownFrac = 0      # Fraction of time SubSystem was Unknown
    self.nBadChanges = 0      # How man times the number of Bad channels changed
    self.nUnknownChanges = 0  # How man times the number of Unknown channels changed
    self.On = "N/A"           # Is the system on

    
#.......................................................................
class SubSystem:

# Define the parameters for a SubSystem

  def __init__(self):
    self.name   = ""           # Name of the SubSystem
    self.desc   = ""           # Description of the SubSystem
    self.chfile = ""           # xml file containing list of channels for this subsystem 
    self.chlist = dict()       # Dictionary of Channel Objects, indexed by Channel name
    self.uthresh = 1           # Number of channels required for unknown status
    self.bthresh = 1           # Number of channels required for bad status
    self.minPerid = 0          # Shortest period of all channels in list
    self.gFrac = 0             # Minimum Good fraction for overall Good status
    self.nUnkwnRateThresh = 0  # Look for SC readout issues 
    self.nUnkwnThresh = 0      # Look for SC readout issues 
    
  #.......................................................................
  def ReadSubSystem(self, TimeStart, TimeStop):
    runTimeDiff = (TimeStop - TimeStart).seconds

    AdjStopTime = TimeStop
    if self.name != "BNB" and self.name != "NuMI" and self.name != "DAQ":
      if runTimeDiff < 2.0*self.minPeriod: AdjStopTime = TimeStop + datetime.timedelta(seconds=self.minPeriod*3.0)
       
    # Loop over list of all Channels for this Subsystem
    for chan in self.chlist:
      # Query SlowMon DB for this Channel
      self.chlist[chan].ReadChannel(TimeStart, AdjStopTime)
      if verbosity == 3: self.chlist[chan].PrintChannel()
  

  #.......................................................................
  def EvalSubSystem(self, TimeStart, TimeStop, Run):
    # Evaluate SubSystem Status and store time-ordered list of changes                                                      
    # If the number of 'Bad' Channels exceeds threshold, then SubSystem Status will be 'Bad'
    # Or if there are no Readings for any Channel, then SubSystem status will be 'Bad'                                               
    # Otherwise if the number of 'Unknown' Channels exceeds threshold, then SubSystem Status will be 'Unknown'    
    # Otherwise SubSystem Status will be 'Good'

    nBad = 0
    nUnknown = 0
    nGood = 0
    nUnknownTrig = 0

    nBadChanges = 0
    nUnknownChanges = 0
    
    SystemStatus = []
    CurrState = dict()
    GlobalSubSystStatus = -2
    GlobalSubSystTime = datetime.datetime(2030,1,1)

    # Store initial SubSystem state based on the initial states of each Channel
    # Create time-ordered list of status changes of every Channel after its initial state
    StatusByTime = []
    for chan in self.chlist:
        CurrState[chan] = [self.chlist[chan].status[0][0],self.chlist[chan].status[0][1]]
        if CurrState[chan][0] == 0: nBad += 1
        elif CurrState[chan][0] == -1: nUnknown += 1
        else: nGood += 1
        GlobalSubSystTime = CurrState[chan][1]

        if (chan == "uB_DAQStatus_DAQX_evb/Builder_TrigRate_NuMI" or chan == "uB_DAQStatus_DAQX_evb/Builder_TrigRate_BNB") and CurrState[chan][0] == -1: nUnknownTrig = 1
        
        clen = len(self.chlist[chan].status)
        if clen < 2: continue
        slen = len(StatusByTime)
        for row in range(1,clen):
            if slen == 0:
                StatusByTime.append([chan,self.chlist[chan].status[row][0],self.chlist[chan].status[row][1]])
                continue
            ctime =self.chlist[chan].status[row][1]
            ndx = len(StatusByTime)
            for l in range(0,len(StatusByTime)):
                ltime = StatusByTime[l][2]
                if ctime < ltime:
                    ndx = l
                    break
            StatusByTime.insert(ndx,[chan,self.chlist[chan].status[row][0],self.chlist[chan].status[row][1]])

    #if nBad >= self.bthresh or ((nBad + nGood) == 0 and len(StatusByTime) == 0): CurrState["Global"] = [0,GlobalSubSystTime]
    if nBad >= self.bthresh: CurrState["Global"] = [0,GlobalSubSystTime]
    elif nUnknown >= self.uthresh or nUnknownTrig == 1: CurrState["Global"] = [-1,GlobalSubSystTime]
    elif nGood == 0:
      if nBad >= nUnknown: CurrState["Global"] = [0,GlobalSubSystTime]
      else: CurrState["Global"] = [-1,GlobalSubSystTime]
    else: CurrState["Global"] = [1,GlobalSubSystTime]
    SystemStatus.append([CurrState["Global"][0], CurrState["Global"][1]])
    nBadPrev = nBad
    nUnknownPrev = nUnknown

            
    if verbosity==4:
      print "Initial State:", CurrState
      print
      print "nGood =", nGood, "nBad =", nBad, "nUnknown =", nUnknown, "Total = ", nGood+nBad+nUnknown
      print 
      print "StatusByTime:", StatusByTime
    # Loop over time-ordred list of Channel state changes and update current state of each Channel
    for l in range(0,len(StatusByTime)):
        CurrState[StatusByTime[l][0]] = [StatusByTime[l][1],StatusByTime[l][2]]
        ctime = StatusByTime[l][2]
        nBad = 0
        nUnknown = 0
        nGood = 0
        nUnknownTrig = 0

        if verbosity == 4:
            print 
            print "******************************"
            print "Channel State Change:", StatusByTime[l]
        # Loop over current state of each Channel and evaluate Global SubSystem status
        for chan in CurrState:
            if chan == "Global": continue
            
            if (chan == "uB_DAQStatus_DAQX_evb/Builder_TrigRate_NuMI" or chan == "uB_DAQStatus_DAQX_evb/Builder_TrigRate_BNB") and CurrState[chan][0] == -1: nUnknownTrig = 1            
            cstat = CurrState[chan][0]
            if cstat == 0: nBad += 1                
            elif cstat == -1: nUnknown += 1
            else: nGood += 1
                
        # If Global SubSystem status has changed, store in list along with TimeStamp of the change
        if nBad >= self.bthresh: GlobalSubSystStatus = 0
        elif nUnknown >= self.uthresh or nUnknownTrig == 1: GlobalSubSystStatus = -1
        elif nGood == 0:
          if nBad >= nUnknown: GlobalSubSystStatus = 0
          else: GlobalSubSystStatus = -1
        else: GlobalSubSystStatus = 1
    
        if nBad != nBadPrev: nBadChanges += 1
        if nUnknown != nUnknownPrev: nUnknownChanges += 1
        nBadPrev = nBad
        nUnknownPrev = nUnknown
    
        if verbosity == 4:
            print
            print "nGood =", nGood, "nBad = ", nBad, "nUnknown = ", nUnknown, "nTotal =", nGood+nBad+nUnknown
        
        if GlobalSubSystStatus != CurrState["Global"][0]:
            GlobalSubSystTime = ctime
            CurrState["Global"] = [GlobalSubSystStatus,GlobalSubSystTime]
            SystemStatus.append([CurrState["Global"][0], CurrState["Global"][1]])
            
            if verbosity == 4:
                print
                print "New Global State:", CurrState["Global"]
        elif verbosity == 4:
            print
            print "No Change in Global State:", CurrState["Global"]


    # Check for SC issues
    rlen = (TimeStop - TimeStart).seconds / 60.0
    if nUnknownChanges > self.nUnkwnThresh and self.nUnkwnThresh > 0 and nUnknownChanges/rlen > self.nUnkwnRateThresh and self.nUnkwnRateThresh > 0: SystemStatus = [[1,TimeStart]]
    
                    
    # Get Good/Bad/Unknown Fractions
    BadFrac = 0
    GoodFrac = 0
    UnknownFrac = 0        
    nStat = len(SystemStatus)
          
    if nStat == 0: UnknownFrac = 1
    elif nStat == 1:
      if SystemStatus[0][0] == 1: GoodFrac = 1
      elif SystemStatus[0][0] == 0: BadFrac = 1
      else: UnknownFrac = 1
    else:
      for l in range(0,nStat):
        if SystemStatus[l][1] > TimeStop: continue
        
        if l < nStat - 1:
          if SystemStatus[l+1][1] > TimeStop: tdiff = TimeStop - SystemStatus[l][1]
          else: tdiff = SystemStatus[l+1][1] - SystemStatus[l][1]
        else: tdiff = TimeStop - SystemStatus[l][1]
          
        if SystemStatus[l][0] == 0: BadFrac += tdiff.seconds + tdiff.microseconds/1e6
        elif SystemStatus[l][0] == 1: GoodFrac += tdiff.seconds + tdiff.microseconds/1e6
        else: UnknownFrac += tdiff.seconds + tdiff.microseconds/1e6

      totTime = GoodFrac + BadFrac + UnknownFrac
      GoodFrac /= totTime
      BadFrac /= totTime
      UnknownFrac /= totTime


    # Create SubSystem status object
    StatusInfo = SubSystemStatus()

    # Include exceptions for some known bad slow mon periods (e.g. high cpu load)
    if self.name == "PMTHV" and ((Run > 4670 and Run < 4703) or (Run > 5742 and Run < 5753) or (Run > 4583 and Run < 4617) or Run == 6167 or (Run > 6187 and Run < 6195) or (Run > 6323 and Run < 6330)):
      SystemStatus = [[1,TimeStart]]
      GoodFrac = 1
      BadFrac = 0
      UnknownFrac = 0

    if (self.name == "TPCWB" or self.name == "Asics" or self.name == "CrateRails" or self.name == "DAQ") and ((Run > 5742 and Run < 5753) or Run == 6167 or (Run > 6187 and Run < 6195) or (Run > 6323 and Run < 6330)) :
      SystemStatus = [[1,TimeStart]]
      GoodFrac = 1
      BadFrac = 0
      UnknownFrac = 0

    # Evaluate overall SubSystem status
    if self.name == "TPCDrift":
      # Check if Drift HV is on
      drAvg = self.chlist['uB_TPCDrift_HV01_1_0/voltage'].avg/1000
      drMax = self.chlist['uB_TPCDrift_HV01_1_0/voltage'].max/1000
      drMin = self.chlist['uB_TPCDrift_HV01_1_0/voltage'].min/1000
      drLo = self.chlist['uB_TPCDrift_HV01_1_0/voltage'].tollo/1000
      drHi = self.chlist['uB_TPCDrift_HV01_1_0/voltage'].tolhi/1000
       
      if ((drMax - drMin) < 0.5 and drAvg > drLo and drAvg <  drHi):
        if len(SystemStatus) == 2 and SystemStatus[0][0] == 1 and SystemStatus[1][0] == -1 and GoodFrac > self.gFrac: StatusInfo.On = "Yes"
        else:
          StatusInfo.On = "Yes"
          SystemStatus = [[1,TimeStart]]
          GoodFrac = 1
          BadFrac = 0
          UnknownFrac = 0
      elif drMax > drLo and drMax < drHi and drMin == 0 and len(SystemStatus) == 2 and GoodFrac > self.gFrac:
        StatusInfo.On = "Yes"
      elif len(SystemStatus) == 1 and SystemStatus[0][0] == -1: StatusInfo.On = "Unknown"
      else: StatusInfo.On = "No"
    else:
      if GoodFrac > self.gFrac: StatusInfo.On = "Yes"
      elif BadFrac >= UnknownFrac: StatusInfo.On = "No"
      else: StatusInfo.On = "Unknown"

    # Include exceptions for some known bad slow mon periods (e.g. high cpu load)    
    if (self.name == "TPCDrift") and ((Run > 5742 and Run < 5753) or (Run > 4583 and Run < 4617) or Run == 6167 or (Run > 6187 and Run < 6195) or (Run > 6323 and Run < 6330)):
      SystemStatus = [[1,TimeStart]]
      GoodFrac = 1
      BadFrac = 0
      UnknownFrac = 0
      StatusInfo.On = "Yes"

        
    # Store and return SubSystem Status
    StatusInfo.StatusList = SystemStatus
    StatusInfo.GoodFrac = GoodFrac
    StatusInfo.BadFrac = BadFrac
    StatusInfo.UnknownFrac = UnknownFrac
    StatusInfo.nBadChanges = nBadChanges
    StatusInfo.nUnknownChanges = nUnknownChanges
    
    return StatusInfo
  
       
#.......................................................................
class Channel:

  # Define the parameters for a SC Channel
  
  def __init__(self):
    self.name   = ""     # Name of the Channel
    self.desc   = ""     # Description of the Channel
    self.status = []     # List of changes to Channel status with time stamps
    self.target = 0      # The nominal value expected for this Channel
    self.tollo  = 0      # The lowest allowed value for this Channel
    self.tolhi  = 0      # The highest allowed value for this Channel
    self.avg = 0         # Average readout value for this Channel
    self.max = 0         # Maximum readout value for this Channel
    self.min = 0         # Minimum readout value for this Channel
    self.period = 1000   # How frequently this Channel is written to SlowMon DB
    self.unit   = ""     # The units of the above values


  #.......................................................................
  def Reset(self):
    self.status = []
    self.avg = 0
    self.max = 0
    self.min = 0


  #.......................................................................
  def ReadChannel(self, TimeStart, TimeStop):
    # Clear status list for this channel
    # Not doing this will mess things up if running over multiple runs
    self.Reset()

    if self.name == "uB_ArPurity_PM02_1/LIFETIME":
      self.ReadLifetime(TimeStart, TimeStart)
      return
    
    cur = smconn.cursor()
    # Setup DB query and execute
    if verbosity == 3: print "Retrieving data for channel", self.name, "between", TimeStart, "--", TimeStop
    #print "Retrieving data for channel", self.name, "between", TimeStart, "--", TimeStop
    try:
      cur.execute("select smpl_time, float_val from sample " 
                  "where channel_id = (select channel_id from channel where name = %s)" 
                  "and smpl_time > %s and smpl_time < %s;",
                  (self.name, TimeStart, TimeStop) )
    except:
      if "uB_BeamData_BEAM_BNB" in self.name: self.ReadBeamChannel(TimeStart,TimeStop)
      else: self.status.append([-1,TimeStart])
      cur.close()
      return

    if cur.rowcount == 0:
      if "uB_BeamData_BEAM_BNB" in self.name: self.ReadBeamChannel(TimeStart,TimeStop)
      else: self.status.append([-1,TimeStart])
      cur.close()
      return


    # Loop over query results
    prevTime = TimeStart
    l = 0
    avgVal = 0
    maxVal = -99999
    minVal = 99999
    r = 0
    for row in cur:
      r += 1
      entryTime = row[0]

      # If time difference between readings is too long, set status to 'Unknown'
      # If this is the first entry for this channel, store Time as Start Time of Run
      tdiff = (entryTime - prevTime).days*86400 + (entryTime - prevTime).seconds
      #print row, tdiff
      if  tdiff > 2*self.period:
        if len(self.status) == 0: self.status.append([-1,prevTime])
        else: self.status.append([-1,prevTime + datetime.timedelta(seconds=2*self.period)])
        prevTime = entryTime
        #continue

      # Evaluate status
      try:
        entryVal = float(row[1])
        avgVal += entryVal
        if entryVal > maxVal: maxVal = entryVal
        if entryVal < minVal: minVal = entryVal
        l += 1
        if entryVal <= self.tollo or entryVal >= self.tolhi: status = 0
        else: status = 1
      except:
        status = -1

      # Store status changes
      # If there are no previously stored entries fot this channel, store Time as Start Time of Run
      # All channels will then have an initial state at Start Time of Run
      if len(self.status) == 0: self.status.append([status,TimeStart])
      elif status != self.status[len(self.status) - 1][0]: self.status.append([status,entryTime])

      tdiff = (TimeStop - entryTime).days*86400 + (TimeStop - entryTime).seconds
      if r == cur.rowcount and status == 1 and tdiff > 2*self.period: self.status.append([-1,entryTime + datetime.timedelta(seconds=2*self.period)])
      prevTime = entryTime

         
    if l>0:
      avgVal /= float(l)
      self.avg = avgVal
      self.max = maxVal
      self.min = minVal
    elif "uB_BeamData_BEAM_BNB" in self.name: self.ReadBeamChannel(TimeStart,TimeStop)

    cur.close()
    
    
  #.......................................................................
  def ReadBeamChannel(self, TimeStart, TimeStop):
    self.Reset()

    chanName = "E:" + self.name[17:].split('_',2)[1].split('/',2)[0]
    try:
        #f = url2.urlopen('http://ifb-data.fnal.gov:8100/ifbeam/data/data?b=BoosterNeutrinoBeam_read&t0=%s&t1=%s&f=csv' % (TimeStart.isoformat(),TimeStop.isoformat()))
        f = url2.urlopen('http://ifb-data.fnal.gov:8100/ifbeam/data/data?v=%s&e=e,1d&t0=%s&t1=%s&f=csv' % (chanName,TimeStart.isoformat(),TimeStop.isoformat()))
    except IOError:
      self.status.append([-1,TimeStart])
      return

      
    re = csv.reader(f)
    l = 0
    avgVal = 0
    maxVal = -99999
    minVal = 99999
    prevTime = TimeStart
    for row in re:
      if l > 0:
        entryTime = datetime.datetime.strptime("1970-01-01 00:00:00.000000","%Y-%m-%d %H:%M:%S.%f") + datetime.timedelta(milliseconds=float(row[2]))
        if l==1:
          offset = datetime.timedelta(hours = (entryTime - TimeStart).days * 24 + ((entryTime - TimeStart).seconds + 1.0)/ 3600)
          if offset.seconds/3600 > 6 or offset.seconds/3600 < 5:
            #print offset.seconds/3600
            self.status.append([-1,TimeStart])
            return

        entryTime = entryTime - offset
        # If time difference between readings is too long, set status to 'Unknown'
        # If this is the first entry for this channel, store Time as Start Time of Run
        tdiff = (entryTime - prevTime).days*86400 + (entryTime - prevTime).seconds
        if  tdiff > 2*self.period:
          if len(self.status) == 0: self.status.append([-1,prevTime])
          else: self.status.append([-1,prevTime + datetime.timedelta(seconds=2*self.period)])
          prevTime = entryTime

        # Evaluate status
        try:
          entryVal = float(row[4])
          avgVal += entryVal
          if entryVal > maxVal: maxVal = entryVal
          if entryVal < minVal: minVal = entryVal
          l += 1
          if entryVal <= self.tollo or entryVal >= self.tolhi: status = 0
          else: status = 1
        except: status = -1
                 
        # Store status changes
        # If there are no previously stored entries fot this channel, store Time as Start Time of Run
        # All channels will then have an initial state at Start Time of Run
        if len(self.status) == 0: self.status.append([status,TimeStart])
        elif status != self.status[len(self.status) - 1][0]: self.status.append([status,entryTime])
        prevTime = entryTime
          
      if l==0: l += 1

    if l < 2:
      if len(self.status) == 0: self.status.append([-1,TimeStart])
      return
    
    avgVal /= float(l)
    self.avg = avgVal
    self.max = maxVal
    self.min = minVal
      

  #.......................................................................
  def ReadLifetime(self, TimeStart, TimeStop):
    cur = smconn.cursor()
    
    # Setup DB query and execute
    if verbosity == 3: print "Retrieving data for channel", self.name, "between", TimeStart, "--", TimeStop
    #print "Retrieving data for channel", self.name, "between", TimeStart, "--", TimeStop
    try:
      cur.execute("select smpl_time, float_val from sample " 
                  "where channel_id = (select channel_id from channel where name = %s)" 
                  "and smpl_time > %s and smpl_time < %s and float_val > 0 and float_val < 0.1",
                  (self.name, TimeStart, TimeStop) )
    except:
      self.status.append([-1,TimeStart])
      cur.close()
      return

    if cur.rowcount == 0:
      cur.execute("select smpl_time, float_val from sample " 
                  "where channel_id = (select channel_id from channel where name = %s)" 
                  "and smpl_time < %s and float_val > 0 and float_val < 0.1 order by smpl_time desc limit 1",
                  (self.name, TimeStart) )

    # Loop over query results
    l = 0
    avgVal = 0
    maxVal = -99999
    minVal = 99999
    for row in cur:
      entryTime = row[0]
      try:
        entryVal = float(row[1])
        avgVal += entryVal
        if entryVal > maxVal: maxVal = entryVal
        if entryVal < minVal: minVal = entryVal
        l += 1
      except: continue
         
    if l>0:
      avgVal /= float(l)
      if avgVal < self.tolhi: self.status.append([1,TimeStart])
      else: self.status.append([0,TimeStart])
    else: self.status.append([-1,TimeStart])
      
    self.avg = avgVal
    self.max = maxVal
    self.min = minVal

    
  #.......................................................................
  def PrintChannel(self):
    clen = len(self.status)
    sys.stdout.write('[')
    for row in range(0,clen):
      sys.stdout.write('[')
      sys.stdout.write(str(self.status[row][0]))
      sys.stdout.write(', ')
      sys.stdout.write(str(self.status[row][1]))
      sys.stdout.write(']')
      if (clen > row+1): sys.stdout.write(', ')
    print ']'

  
#.......................................................................
def ConnectToSMDB():
    #-- Connect to SlowMon DB
    #   If password isn't given, psycopg2 will obtain it from
    #   $HOME/.pgpass if it exists.
    global smconn
    try:
      smconn = psycopg2.connect(host="", user="", port="",
                                database="", password="")    
    except:
      print "Connection to SlowMon database could not be established"
      return False

    return True

  
#.......................................................................
def ConnectToRCDB():
    #-- Connect to RunConfig DB
    #   If password isn't given, psycopg2 will obtain it from
    #   $HOME/.pgpass if it exists.
    global rcconn
    try:
      rcconn = psycopg2.connect(host="", user="", port="",
                                database="", password="")    
    except:
      print "Connection to RunConfig database could not be established"
      return False

    return True

  
#.......................................................................
def ConnectToGRDB():
  global grconn
  try:
    grconn = sqlite3.connect('GoodRuns.db')
  except:
    print "Connection to GoodRuns database could not be established"
    return False

  return True


#.......................................................................
def LoadSubSystemList(ssfile):
    # Load list of Subsystems from xml file and store parameters in a dictionary
    tree = ET.parse(ssfile)
    root = tree.getroot()

    subSystList = dict()
    for child in root:
        if child.tag != 'subsystem': return
        nm = child.attrib.get("name",None) 
        desc = child.attrib.get("desc",None)
        chfile = child.attrib.get("chlist",None)
        uthresh = child.attrib.get("uthresh",None)
        bthresh = child.attrib.get("bthresh",None)
        gFrac = child.attrib.get("gFrac",None)
        nUnkwnRateThresh = child.attrib.get("nUnkwnRateThresh",None)
        nUnkwnThresh = child.attrib.get("nUnkwnThresh",None)

        subSystList[nm]        = SubSystem()
        subSystList[nm].name   = str(nm)
        subSystList[nm].desc   = str(desc)
        subSystList[nm].chfile = str(chfile)
        subSystList[nm].chlist, subSystList[nm].minPeriod = LoadChannelList(subSystList[nm].chfile)
        subSystList[nm].uthresh = int(uthresh)
        subSystList[nm].bthresh = int(bthresh)
        subSystList[nm].gFrac = float(gFrac)
        subSystList[nm].nUnkwnRateThresh = float(nUnkwnRateThresh)
        subSystList[nm].nUnkwnThresh = float(nUnkwnThresh)

        if subSystList[nm].name == 'PMTHV': subSystList[nm].minPeriod = 2.0*subSystList[nm].minPeriod
             
    return subSystList

  
#.......................................................................
def LoadChannelList(chfile):
    # Load list of Channels from xml file and store parameters in a dictionary
    tree = ET.parse(chfile)
    root = tree.getroot()

    minPeriod = 9999.0
    channelList = dict()
    for child in root:
        if child.tag != 'channel': return
        nm = child.attrib.get("name",None)
        desc = child.attrib.get("desc",None)
        target = child.attrib.get("target",None)
        tolhi = child.attrib.get("tolhi",None)
        tollo = child.attrib.get("tollo",None)
        period = child.attrib.get("period",None)
        unit = child.attrib.get("unit",None)

        if float(period) < minPeriod: minPeriod = float(period)

        channelList[nm]        = Channel()
        channelList[nm].name   = str(nm)
        channelList[nm].desc   = str(desc)
        channelList[nm].target = float(target)
        channelList[nm].tolhi  = float(tolhi)
        channelList[nm].tollo  = float(tollo)
        channelList[nm].period = float(period)
        channelList[nm].unit   = str(unit)

    return channelList, minPeriod

  
#.......................................................................
def PrintSubSystemList(subSystList):
    for syst in subSystList:
        print " "
        print syst, ":", subSystList[syst].desc
        for chan in subSystList[syst].chlist:
            print "  ", chan, ":", subSystList[syst].chlist[chan].desc, ":", subSystList[syst].chlist[chan].target, ":", subSystList[syst].chlist[chan].tolhi, ":", subSystList[syst].chlist[chan].tollo, ":", subSystList[syst].chlist[chan].period, ":", subSystList[syst].chlist[chan].unit


#.......................................................................
def CheckDBForRun(RunNumber):
    cur = grconn.cursor()

    # Search for entries with this run number
    t = (RunNumber,)
    cur.execute('select run_number from RunStatus where version = (select max(version) from RunStatus) and run_number = ?', t)
    rt = cur.fetchone()
    try:
      int(rt[0])
      return True
    except: return False
        
  
#.......................................................................
if __name__ == "__main__":
  global verbosity
  global test_mode
  # verbosity levels:
  # 1 - Print procedure
  # 2 - Print procedure, DB queries, run info, subsystem status, run status
  # 3 - Print procedure, DB queries, run info, channel status, run status
  # 4 - Print procedure, DB queries, run info, subsystem eval debugging, run status
  # 5 - Print procedure, DB queries, run info, run eval debugging , run status
  # 6 - Print procedure, DB queries, subsystem/channel lists, run info, run status


  # Create argument parser
  parser = argparse.ArgumentParser()
  parser.add_argument('--verbose', '-v', action='count', default=0, help='Increase verbosity level')  # Increase verbosity
  parser.add_argument('--test', '-t', action='store_true')                                            # Run in test mode
  parser.add_argument('--missing', '-m', action='store_true')                                         # Evaluate only runs not already in DB
  parser.add_argument('--ntuple', '-n', action='store_true')                                          # Write ntuple
  parser.add_argument('run', nargs='*', type=int, help='Run number to evaluate')                      # List of 1 or more Run Numbers
  parser.add_argument('--range', '-r', nargs=2, type=int, help='Range of run numbers to evaluate')    # Range of Run Numbers

  # Require at least one argument, a Run Number
  if len(sys.argv) < 2:
    parser.print_usage()
    sys.exit(1)

  # Parse command line arguments
  args = parser.parse_args()
  verbosity = args.verbose
  test_mode = args.test
  missing_only = args.missing
  write_tuple = args.ntuple
  RunList = args.run[:]
  if args.range != None:
    if args.range[1] < args.range[0]:
      print "Invalid Run Range:", args.range[0], ">", args.range[1]
      parser.print_usage()
      sys.exit(1)
    for r in range(args.range[0],args.range[1]+1):
      if not (r in set(RunList)): RunList.append(r)

        
  # Connect to SlowMon database
  if verbosity > 0: print "Connecting to SlowMon database..."
  if ConnectToSMDB() == False: sys.exit(1)
  elif verbosity > 0: print "Connection established"

  # Connect to RunConfig database
  if verbosity > 0: print "Connecting to RunConfig database..."
  if ConnectToRCDB() == False: sys.exit(1)
  elif verbosity > 0: print "Connection established"

  # Connect to GoodRuns sqlite database
  if ConnectToGRDB() == False: sys.exit(1)

  # Load list of subsystems and channels for each subsystem
  if verbosity > 0: print "Loading list of subsystems and channels"
  ssfile = "SubSystems.xml"
  subSystList = LoadSubSystemList(ssfile)
  if verbosity == 6: PrintSubSystemList(subSystList)

  if write_tuple: tuple_name = "RunTuple_%i-%i.txt" % (RunList[0], RunList[len(RunList) -1])
    
  # Loop over list of runs to process
  for r in range(0,len(RunList)):
    # Check DB if only evaluating missing runs
    if missing_only:
      if CheckDBForRun(RunList[r]): continue
    
    # Create Run Information container object
    CurrentRun = RunInfo(RunList[r])
    CurrentRun.InitSubSystStatus(subSystList)
    if verbosity > 0: print "Retrieving configuration for Run", RunList[r], "..."

    runInfoRes = CurrentRun.GetRunConfig()
    if runInfoRes == -1:
      print "Run ", RunList[r], " is not in RunConfigDB"
      del CurrentRun
      continue
    elif runInfoRes == 0:
      print "Could not find configuration for Run ", RunList[r]
      if verbosity > 1: CurrentRun.PrintRunStatusByTime()
      if not test_mode:
        CurrentRun.WriteToDB(subSystList)
        CurrentRun.WriteToWeb(subSystList)
      del CurrentRun
      continue
    elif verbosity > 0: print "Sucess"

    if verbosity > 0: print "Determing Run Boundary..."
    runBoundRes = CurrentRun.GetRunBoundary()
    if runBoundRes == -1:
      print "Run ", RunList[r], "seems to have Crashed or been Stopped with no Data taken"
      CurrentRun.GoodForPhysAna = "No"
      if not test_mode:
        CurrentRun.WriteToDB(subSystList)
        CurrentRun.WriteToWeb(subSystList)
      del CurrentRun
      continue  
    if runBoundRes == 0:
      print "Unable to determine Stop Time of Run", RunList[r], "Still running?"
      del CurrentRun
      continue
    elif verbosity > 0: print "Success"

    if verbosity > 1: CurrentRun.PrintRunInfo()

    # Read from SlowMon database, evaluate state of subsystems and Run
    if verbosity > 0: print "Reading from SlowMon Database for Run", RunList[r]
    CurrentRun.ReadSubSystems(subSystList)

    if verbosity > 0: print "Evaluating Status of Run", RunList[r]
    CurrentRun.CheckRunCat()
    CurrentRun.CheckDetPower()
    CurrentRun.EvalRun()
    CurrentRun.FindGoodSubRuns()
    
    if verbosity > 1:
      CurrentRun.PrintRunStatus(subSystList)
      CurrentRun.PrintRunStatusByTime()
    if not test_mode:
      print "\nStoring Results for Run", RunList[r]
      CurrentRun.WriteToDB(subSystList)
      CurrentRun.WriteToWeb(subSystList)
    if write_tuple: CurrentRun.WriteToTuple(subSystList, tuple_name)
    del CurrentRun

    
  # Close DB Connections
  smconn.close()
  rcconn.close()
  grconn.close()
