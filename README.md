# foo_rawtap — Raw Sample Tap for foobar2000

`foo_rawtap` is a foobar2000 component that streams the audio being played, as raw 32-bit
floating-point PCM, to a Windows named pipe. Any program on the same machine can open the
pipe and receive the samples while music is playing. The component is written in C++ against
the foobar2000 SDK.

Typical uses are waveform and oscilloscope displays, level meters, signal analysis, and
feeding another program with the samples foobar2000 is playing, without a loopback driver or
a virtual audio cable.
