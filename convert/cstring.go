package convert

type Cstring string

func (cs *Cstring) Unpack(reader io.Reader, _ int, _ *struc.Options) error {
	buffer := []byte{0}
	stringBuffer := []byte{}
	for {
		n, err := reader.Read(buffer)
		if err != nil {
			return fmt.Errorf("error reading null string from connection: %v", err)
		}
		if n != 1 {
			return fmt.Errorf("insufficient bytes read")
		}
		if buffer[0] == '\x00' {
			break
		}
		stringBuffer = append(stringBuffer, buffer[0])
	}

	*cs = cstring(stringBuffer)

	return nil
}

func (cs *Cstring) Pack(bslice []byte, _ *struc.Options) (int, error) {
	panic("unimplemented")
	/*
	cstrBytes = []byte(*cs)
	copy(bslice, cstrBytes)
	bslice[ len(cstrBytes) ] = 0
	*/
}

func (cs *Cstring) Size(_ *struc.Options) int {
	return len(cs.String())
}

func (cs *Cstring) String() string {
	return string(*cs)
}
