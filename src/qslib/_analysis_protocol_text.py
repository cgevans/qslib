# SPDX-FileCopyrightText: 2021-2022 Constantine Evans <const@costi.eu>
#
# SPDX-License-Identifier: AGPL-3.0-only

_ANALYSIS_PROTOCOL_TEXT = """<JaxbAnalysisProtocol>
    <Name>unnamed</Name>
    <JaxbAnalysisSettings>
        <Type>com.apldbio.sds.platform.core.analysis.IDetectorSettings</Type>
        <JaxbSettingValue>
            <Name>ConfidenceLevel</Name>
            <JaxbValueItem type="Double">
                <DoubleValue>99.0</DoubleValue>
            </JaxbValueItem>
        </JaxbSettingValue>
        <JaxbSettingValue>
            <Name>CreatedByUser</Name>
            <JaxbValueItem type="Boolean">
                <BooleanValue>true</BooleanValue>
            </JaxbValueItem>
        </JaxbSettingValue>
        <JaxbSettingValue>
            <Name>Threshold</Name>
            <JaxbValueItem type="Double">
                <DoubleValue>0.2</DoubleValue>
            </JaxbValueItem>
        </JaxbSettingValue>
        <JaxbSettingValue>
            <Name>AutoBaseline</Name>
            <JaxbValueItem type="Boolean">
                <BooleanValue>true</BooleanValue>
            </JaxbValueItem>
        </JaxbSettingValue>
        <JaxbSettingValue>
            <Name>BaselineStart</Name>
            <JaxbValueItem type="Integer">
                <IntValue>3</IntValue>
            </JaxbValueItem>
        </JaxbSettingValue>
        <JaxbSettingValue>
            <Name>ObjectName</Name>
            <JaxbValueItem type="String">
                <StringValue>AnalysisProtocol.DEFAULT_SETTINGS</StringValue>
            </JaxbValueItem>
        </JaxbSettingValue>
        <JaxbSettingValue>
            <Name>BaselineStop</Name>
            <JaxbValueItem type="Integer">
                <IntValue>15</IntValue>
            </JaxbValueItem>
        </JaxbSettingValue>
        <JaxbSettingValue>
            <Name>AutoCt</Name>
            <JaxbValueItem type="Boolean">
                <BooleanValue>true</BooleanValue>
            </JaxbValueItem>
        </JaxbSettingValue>
    </JaxbAnalysisSettings>
    <JaxbAnalysisSettings>
        <Type>com.apldbio.sds.platform.core.analysis.IWellSettings</Type>
        <JaxbSettingValue>
            <Name>BaselineStart</Name>
            <JaxbValueItem type="Integer">
                <IntValue>3</IntValue>
            </JaxbValueItem>
        </JaxbSettingValue>
        <JaxbSettingValue>
            <Name>ObjectName</Name>
            <JaxbValueItem type="String">
                <StringValue>AnalysisProtocol.DEFAULT_SETTINGS</StringValue>
            </JaxbValueItem>
        </JaxbSettingValue>
        <JaxbSettingValue>
            <Name>BaselineStop</Name>
            <JaxbValueItem type="Integer">
                <IntValue>15</IntValue>
            </JaxbValueItem>
        </JaxbSettingValue>
        <JaxbSettingValue>
            <Name>UseDetectorDefaults</Name>
            <JaxbValueItem type="Boolean">
                <BooleanValue>false</BooleanValue>
            </JaxbValueItem>
        </JaxbSettingValue>
    </JaxbAnalysisSettings>
    <JaxbAnalysisSettings>
        <Type>com.apldbio.sds.platform.core.analysis.ISignalSmoothingSettings</Type>
        <JaxbSettingValue>
            <Name>SignalSmoothing</Name>
            <JaxbValueItem type="Boolean">
                <BooleanValue>true</BooleanValue>
            </JaxbValueItem>
        </JaxbSettingValue>
        <JaxbSettingValue>
            <Name>ObjectName</Name>
            <JaxbValueItem type="String">
                <StringValue>AnalysisProtocol.DEFAULT_SETTINGS</StringValue>
            </JaxbValueItem>
        </JaxbSettingValue>
    </JaxbAnalysisSettings>
    <JaxbAnalysisSettings>
        <Type>com.apldbio.sds.platform.core.analysis.IAlgorithmSelectSettings</Type>
        <JaxbSettingValue>
            <Name>AlgorithmName</Name>
            <JaxbValueItem type="String">
                <StringValue>default</StringValue>
            </JaxbValueItem>
        </JaxbSettingValue>
        <JaxbSettingValue>
            <Name>ObjectName</Name>
            <JaxbValueItem type="String">
                <StringValue>AnalysisProtocol.DEFAULT_SETTINGS</StringValue>
            </JaxbValueItem>
        </JaxbSettingValue>
    </JaxbAnalysisSettings>
    <JaxbAnalysisSettings>
        <Type>com.apldbio.sds.platform.core.analysis.IAutoAnalysisSettings</Type>
        <JaxbSettingValue>
            <Name>AutoAnalysis</Name>
            <JaxbValueItem type="Boolean">
                <BooleanValue>false</BooleanValue>
            </JaxbValueItem>
        </JaxbSettingValue>
        <JaxbSettingValue>
            <Name>AutoSpectral</Name>
            <JaxbValueItem type="Boolean">
                <BooleanValue>false</BooleanValue>
            </JaxbValueItem>
        </JaxbSettingValue>
        <JaxbSettingValue>
            <Name>ObjectName</Name>
            <JaxbValueItem type="String">
                <StringValue>AnalysisProtocol.DEFAULT_SETTINGS</StringValue>
            </JaxbValueItem>
        </JaxbSettingValue>
    </JaxbAnalysisSettings>
    <JaxbAnalysisSettings>
        <Type>com.apldbio.sds.platform.core.analysis.IDataSelectSettings</Type>
        <JaxbSettingValue>
            <Name>StepNum</Name>
            <JaxbValueItem type="Integer">
                <IntValue>5</IntValue>
            </JaxbValueItem>
        </JaxbSettingValue>
        <JaxbSettingValue>
            <Name>StageNum</Name>
            <JaxbValueItem type="Integer">
                <IntValue>4</IntValue>
            </JaxbValueItem>
        </JaxbSettingValue>
        <JaxbSettingValue>
            <Name>MeltStageNum</Name>
            <JaxbValueItem type="Integer">
                <IntValue>0</IntValue>
            </JaxbValueItem>
        </JaxbSettingValue>
        <JaxbSettingValue>
            <Name>ObjectName</Name>
            <JaxbValueItem type="String">
                <StringValue>IDataSelectSettings.OBJECT_NAME</StringValue>
            </JaxbValueItem>
        </JaxbSettingValue>
        <JaxbSettingValue>
            <Name>PointNum</Name>
            <JaxbValueItem type="Integer">
                <IntValue>5</IntValue>
            </JaxbValueItem>
        </JaxbSettingValue>
    </JaxbAnalysisSettings>
    <JaxbAnalysisSettings>
        <Type>com.apldbio.sds.platform.core.analysis.ICrtSettings</Type>
        <JaxbSettingValue>
            <Name>CrtStartValue</Name>
            <JaxbValueItem type="Integer">
                <IntValue>1</IntValue>
            </JaxbValueItem>
        </JaxbSettingValue>
    </JaxbAnalysisSettings>
    <AnalysisSetting>
        <Type>NHC</Type>
        <SettingValue>
            <Name>enabled</Name>
            <Type>Boolean</Type>
            <BooleanValue>true</BooleanValue>
        </SettingValue>
        <SettingValue>
            <Name>criteria</Name>
            <Type>String</Type>
            <StringValue>LESS_THAN 35.0</StringValue>
        </SettingValue>
        <SettingValue>
            <Name>omitEnabled</Name>
            <Type>Boolean</Type>
            <BooleanValue>false</BooleanValue>
        </SettingValue>
    </AnalysisSetting>
    <AnalysisSetting>
        <Type>BPR</Type>
        <SettingValue>
            <Name>enabled</Name>
            <Type>Boolean</Type>
            <BooleanValue>true</BooleanValue>
        </SettingValue>
        <SettingValue>
            <Name>criteria</Name>
            <Type>String</Type>
            <StringValue>GREATER_THAN 0.6</StringValue>
        </SettingValue>
        <SettingValue>
            <Name>omitEnabled</Name>
            <Type>Boolean</Type>
            <BooleanValue>false</BooleanValue>
        </SettingValue>
    </AnalysisSetting>
    <AnalysisSetting>
        <Type>DRNMIN</Type>
        <SettingValue>
            <Name>enabled</Name>
            <Type>Boolean</Type>
            <BooleanValue>true</BooleanValue>
        </SettingValue>
        <SettingValue>
            <Name>criteria</Name>
            <Type>String</Type>
            <StringValue>LESS_THAN 35.0</StringValue>
        </SettingValue>
        <SettingValue>
            <Name>criteria</Name>
            <Type>String</Type>
            <StringValue>LESS_THAN 0.2</StringValue>
        </SettingValue>
        <SettingValue>
            <Name>omitEnabled</Name>
            <Type>Boolean</Type>
            <BooleanValue>false</BooleanValue>
        </SettingValue>
    </AnalysisSetting>
    <AnalysisSetting>
        <Type>FOS</Type>
        <SettingValue>
            <Name>enabled</Name>
            <Type>Boolean</Type>
            <BooleanValue>true</BooleanValue>
        </SettingValue>
        <SettingValue>
            <Name>criteria</Name>
            <Type>String</Type>
            <StringValue>true</StringValue>
        </SettingValue>
        <SettingValue>
            <Name>omitEnabled</Name>
            <Type>Boolean</Type>
            <BooleanValue>false</BooleanValue>
        </SettingValue>
    </AnalysisSetting>
    <AnalysisSetting>
        <Type>HSD</Type>
        <SettingValue>
            <Name>enabled</Name>
            <Type>Boolean</Type>
            <BooleanValue>true</BooleanValue>
        </SettingValue>
        <SettingValue>
            <Name>criteria</Name>
            <Type>String</Type>
            <StringValue>GREATER_THAN 0.5</StringValue>
        </SettingValue>
        <SettingValue>
            <Name>omitEnabled</Name>
            <Type>Boolean</Type>
            <BooleanValue>false</BooleanValue>
        </SettingValue>
    </AnalysisSetting>
    <AnalysisSetting>
        <Type>CC</Type>
        <SettingValue>
            <Name>enabled</Name>
            <Type>Boolean</Type>
            <BooleanValue>true</BooleanValue>
        </SettingValue>
        <SettingValue>
            <Name>criteria</Name>
            <Type>String</Type>
            <StringValue>LESS_THAN 0.8</StringValue>
        </SettingValue>
        <SettingValue>
            <Name>omitEnabled</Name>
            <Type>Boolean</Type>
            <BooleanValue>false</BooleanValue>
        </SettingValue>
    </AnalysisSetting>
    <AnalysisSetting>
        <Type>NA</Type>
        <SettingValue>
            <Name>enabled</Name>
            <Type>Boolean</Type>
            <BooleanValue>true</BooleanValue>
        </SettingValue>
        <SettingValue>
            <Name>criteria</Name>
            <Type>String</Type>
            <StringValue>LESS_THAN 0.1</StringValue>
        </SettingValue>
        <SettingValue>
            <Name>omitEnabled</Name>
            <Type>Boolean</Type>
            <BooleanValue>false</BooleanValue>
        </SettingValue>
    </AnalysisSetting>
    <AnalysisSetting>
        <Type>HRN</Type>
        <SettingValue>
            <Name>enabled</Name>
            <Type>Boolean</Type>
            <BooleanValue>true</BooleanValue>
        </SettingValue>
        <SettingValue>
            <Name>criteria</Name>
            <Type>String</Type>
            <StringValue>GREATER_THAN 4.0</StringValue>
        </SettingValue>
        <SettingValue>
            <Name>omitEnabled</Name>
            <Type>Boolean</Type>
            <BooleanValue>false</BooleanValue>
        </SettingValue>
    </AnalysisSetting>
    <AnalysisSetting>
        <Type>NS</Type>
        <SettingValue>
            <Name>enabled</Name>
            <Type>Boolean</Type>
            <BooleanValue>true</BooleanValue>
        </SettingValue>
        <SettingValue>
            <Name>criteria</Name>
            <Type>String</Type>
            <StringValue>GREATER_THAN 1.0</StringValue>
        </SettingValue>
        <SettingValue>
            <Name>omitEnabled</Name>
            <Type>Boolean</Type>
            <BooleanValue>false</BooleanValue>
        </SettingValue>
    </AnalysisSetting>
    <AnalysisSetting>
        <Type>EW</Type>
        <SettingValue>
            <Name>enabled</Name>
            <Type>Boolean</Type>
            <BooleanValue>false</BooleanValue>
        </SettingValue>
        <SettingValue>
            <Name>criteria</Name>
            <Type>String</Type>
            <StringValue>true</StringValue>
        </SettingValue>
        <SettingValue>
            <Name>omitEnabled</Name>
            <Type>Boolean</Type>
            <BooleanValue>false</BooleanValue>
        </SettingValue>
        <SettingValue>
            <Name>criteria4QS</Name>
            <Type>String</Type>
            <StringValue>LESS_THAN 100000.0</StringValue>
        </SettingValue>
    </AnalysisSetting>
    <AnalysisSetting>
        <Type>ORG</Type>
        <SettingValue>
            <Name>enabled</Name>
            <Type>Boolean</Type>
            <BooleanValue>true</BooleanValue>
        </SettingValue>
        <SettingValue>
            <Name>criteria</Name>
            <Type>String</Type>
            <StringValue>true</StringValue>
        </SettingValue>
        <SettingValue>
            <Name>omitEnabled</Name>
            <Type>Boolean</Type>
            <BooleanValue>false</BooleanValue>
        </SettingValue>
    </AnalysisSetting>
    <AnalysisSetting>
        <Type>EAF</Type>
        <SettingValue>
            <Name>enabled</Name>
            <Type>Boolean</Type>
            <BooleanValue>true</BooleanValue>
        </SettingValue>
        <SettingValue>
            <Name>criteria</Name>
            <Type>String</Type>
            <StringValue>true</StringValue>
        </SettingValue>
        <SettingValue>
            <Name>omitEnabled</Name>
            <Type>Boolean</Type>
            <BooleanValue>false</BooleanValue>
        </SettingValue>
    </AnalysisSetting>
    <AnalysisSetting>
        <Type>BAF</Type>
        <SettingValue>
            <Name>enabled</Name>
            <Type>Boolean</Type>
            <BooleanValue>true</BooleanValue>
        </SettingValue>
        <SettingValue>
            <Name>criteria</Name>
            <Type>String</Type>
            <StringValue>true</StringValue>
        </SettingValue>
        <SettingValue>
            <Name>omitEnabled</Name>
            <Type>Boolean</Type>
            <BooleanValue>false</BooleanValue>
        </SettingValue>
    </AnalysisSetting>
    <AnalysisSetting>
        <Type>TAF</Type>
        <SettingValue>
            <Name>enabled</Name>
            <Type>Boolean</Type>
            <BooleanValue>true</BooleanValue>
        </SettingValue>
        <SettingValue>
            <Name>criteria</Name>
            <Type>String</Type>
            <StringValue>true</StringValue>
        </SettingValue>
        <SettingValue>
            <Name>omitEnabled</Name>
            <Type>Boolean</Type>
            <BooleanValue>false</BooleanValue>
        </SettingValue>
    </AnalysisSetting>
    <AnalysisSetting>
        <Type>CAF</Type>
        <SettingValue>
            <Name>enabled</Name>
            <Type>Boolean</Type>
            <BooleanValue>true</BooleanValue>
        </SettingValue>
        <SettingValue>
            <Name>criteria</Name>
            <Type>String</Type>
            <StringValue>true</StringValue>
        </SettingValue>
        <SettingValue>
            <Name>omitEnabled</Name>
            <Type>Boolean</Type>
            <BooleanValue>false</BooleanValue>
        </SettingValue>
    </AnalysisSetting>
    <AnalysisSetting>
        <Type>ROXLOW</Type>
        <SettingValue>
            <Name>enabled</Name>
            <Type>Boolean</Type>
            <BooleanValue>true</BooleanValue>
        </SettingValue>
        <SettingValue>
            <Name>criteria</Name>
            <Type>String</Type>
            <StringValue>LESS_THAN 20.0</StringValue>
        </SettingValue>
        <SettingValue>
            <Name>omitEnabled</Name>
            <Type>Boolean</Type>
            <BooleanValue>false</BooleanValue>
        </SettingValue>
    </AnalysisSetting>
    <AnalysisSetting>
        <Type>ROXDROP</Type>
        <SettingValue>
            <Name>enabled</Name>
            <Type>Boolean</Type>
            <BooleanValue>true</BooleanValue>
        </SettingValue>
        <SettingValue>
            <Name>criteria</Name>
            <Type>String</Type>
            <StringValue>GREATER_THAN 0.04</StringValue>
        </SettingValue>
        <SettingValue>
            <Name>omitEnabled</Name>
            <Type>Boolean</Type>
            <BooleanValue>false</BooleanValue>
        </SettingValue>
    </AnalysisSetting>
</JaxbAnalysisProtocol>
"""
